# python-script (replayed against instantd v2)

Replay of v1's `examples/python-script` using the **frozen PyPI SDK**
(`instantdb==1.0.65`, unmodified). The only delta from the upstream example:
`api_uri` comes from `INSTANT_API_URI` so self-hosted instantd can be targeted.

Run:

```sh
uv venv --python 3.12 && uv pip install instantdb httpx python-dotenv
export INSTANT_API_URI=http://127.0.0.1:18891
export INSTANT_APP_ID=<uuid> INSTANT_APP_ADMIN_TOKEN=<uuid>
python main.py
```

Verified against instantd: high-level tx ops (`update`/`merge`) with on-demand
attr provisioning, bare object-tree `/admin/query` responses, where-filters,
and (via the same SDK) live subscriptions over `POST /admin/subscribe-query`
SSE with push-on-write invalidation.
