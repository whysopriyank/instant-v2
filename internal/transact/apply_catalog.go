package transact

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
)

// prepareCatalog provisions same-batch attrs before lookup resolution and
// rewrites aliases to stored IDs without changing the shared catalog.
func prepareCatalog(ctx context.Context, tx pgx.Tx, appID [16]byte, catalog *platform.AttrCatalog, steps []Step, opts Options, ruleDoc *perms.RuleDoc) (*platform.AttrCatalog, error) {
	txCat := catalog
	if hasOp(steps, "add-attr") || hasOp(steps, "update-attr") {
		txCat = catalog.Clone()
		// clientAttrID → serverAttrID for attrs the server already
		// stores under another id (replayed or implicit-attr replays,
		// e.g. <etype>/id). Mirrors the TS client's _rewriteMutations.
		attrAlias := map[[16]byte][16]byte{}
		for _, st := range steps {
			if st.Op != "add-attr" {
				continue
			}
			a, err := parseWireAttr(st.Args[0])
			if err != nil {
				return nil, fmt.Errorf("transact: add-attr: %w", err)
			}
			if err := validateAddRequired(ctx, tx, appID, a); err != nil {
				return nil, fmt.Errorf("transact: add-attr: %w", err)
			}
			existing, found, err := platform.FindAttrByIdent(ctx, tx, appID, deref(a.Etype), deref(a.Label))
			if err != nil {
				return nil, fmt.Errorf("transact: add-attr: %w", err)
			}
			if found {
				// Adopt the stored definition; alias the client's id so
				// same-batch triples resolve and are rewritten below.
				attrAlias[a.ID] = existing.ID
				txCat.Add(existing)
				aliased := existing
				aliased.ID = a.ID
				txCat.Add(aliased)
				continue
			}
			if !opts.Admin && ruleDoc != nil {
				allow, err := perms.Check("attrs", "create", ruleDoc, perms.Bindings{
					Auth: opts.AuthUser, RuleParams: opts.RuleParams, Request: opts.Request,
				})
				if err != nil {
					return nil, fmt.Errorf("transact: attrs.create: %w", err)
				}
				if !allow {
					return nil, fmt.Errorf("transact: attrs.create denied")
				}
			}
			if err := platform.CreateAttrWithID(ctx, tx, appID, a); err != nil {
				return nil, fmt.Errorf("transact: add-attr: %w", err)
			}
			txCat.Add(a)
		}
		if len(attrAlias) > 0 {
			rewriteAttrRefs(steps, attrAlias)
		}
	}

	return txCat, nil
}

// applyRequiredAttrUpdates applies the supported update-attr patch and
// returns the resulting rows for the post-write coverage check. Metadata
// updates are kept on the active transaction so a failed coverage check rolls
// back together with the caller's data writes.
func applyRequiredAttrUpdates(ctx context.Context, tx pgx.Tx, appID [16]byte, cat *platform.AttrCatalog, batch []Step, opts Options, ruleDoc *perms.RuleDoc) ([]platform.Attr, error) {
	updates := make([]platform.Attr, 0, len(batch))
	for _, st := range batch {
		if len(st.Args) != 1 {
			return nil, fmt.Errorf("transact: update-attr: want one payload")
		}
		patch, err := parseRequiredAttrUpdate(st.Args[0])
		if err != nil {
			return nil, fmt.Errorf("transact: update-attr: %w", err)
		}
		var id [16]byte
		if err := parseUUID(patch.ID, &id); err != nil {
			return nil, fmt.Errorf("transact: update-attr: %w", err)
		}
		current, found, err := platform.FindAttrByID(ctx, tx, appID, id)
		if err != nil {
			return nil, fmt.Errorf("transact: update-attr: %w", err)
		}
		if !found {
			return nil, fmt.Errorf("transact: update-attr: unknown attr %s", patch.ID)
		}
		// Both identity directions are security-sensitive namespace gates. The
		// stored row, rather than only the wire payload, is authoritative here:
		// update-attr intentionally has no identity fields, so a forged client
		// payload cannot bypass a reserved reverse etype either.
		if current.Etype != nil && perms.ReservedNamespaces[*current.Etype] {
			return nil, fmt.Errorf("transact: update-attr: %q is a reserved namespace", *current.Etype)
		}
		if current.ReverseEtype != nil && perms.ReservedNamespaces[*current.ReverseEtype] {
			return nil, fmt.Errorf("transact: update-attr: reverse namespace %q is reserved", *current.ReverseEtype)
		}
		// update-attr is an attrs-level operation, not an entity mutation, so
		// enforcePerms does not see it. Keep the same fail-closed rule gate and
		// bindings as the add-attr path. Admin bypasses this permission only;
		// namespace gates above still apply.
		if !opts.Admin && ruleDoc != nil {
			allow, err := perms.Check("attrs", "update", ruleDoc, perms.Bindings{
				Auth: opts.AuthUser, RuleParams: opts.RuleParams, Request: opts.Request,
			})
			if err != nil {
				return nil, fmt.Errorf("transact: attrs.update: %w", err)
			}
			if !allow {
				return nil, fmt.Errorf("transact: attrs.update denied")
			}
		}
		current.IsRequired = *patch.Required
		if _, err := tx.Exec(ctx, `
			UPDATE attrs
			   SET is_required = $1
			 WHERE app_id = $2 AND id = $3 AND deletion_marked_at IS NULL`,
			current.IsRequired, appID, id); err != nil {
			return nil, fmt.Errorf("transact: update-attr: %w", err)
		}
		cat.Add(current)
		updates = append(updates, current)
	}
	return updates, nil
}

// hasOp reports whether any step carries the given op.
func hasOp(steps []Step, op string) bool {
	for _, st := range steps {
		if st.Op == op {
			return true
		}
	}
	return false
}

// rewriteAttrRefs replaces client-minted attr ids in triple steps with the
// server-side ids adopted during add-attr processing. Only Args[1] of
// triple-shaped ops participates — the position parseTripleArgs reads.
func rewriteAttrRefs(steps []Step, alias map[[16]byte][16]byte) {
	if len(alias) == 0 {
		return
	}
	remap := func(raw json.RawMessage) (json.RawMessage, bool) {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return raw, false
		}
		var id [16]byte
		if parseUUID(s, &id) != nil {
			return raw, false
		}
		serverID, ok := alias[id]
		if !ok {
			return raw, false
		}
		out, err := json.Marshal(uuidToStr(serverID))
		if err != nil {
			return raw, false
		}
		return out, true
	}
	for i := range steps {
		switch steps[i].Op {
		case "add-triple", "deep-merge-triple", "retract-triple":
			if len(steps[i].Args) >= 3 {
				if repl, ok := remap(steps[i].Args[1]); ok {
					steps[i].Args[1] = repl
				}
			}
		}
	}
}
