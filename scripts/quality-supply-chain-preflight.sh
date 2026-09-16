#!/usr/bin/env bash
# Offline, read-only QR-005 supply-chain input reconciliation.
set -u -o pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
if (($# > 1)); then
  printf '%s\n' 'usage: quality-supply-chain-preflight.sh [repository-root]' >&2
  exit 2
fi
repo_root=$(cd "${1:-$script_dir/..}" && pwd)

if ! command -v jq >/dev/null 2>&1; then
  printf '%s\n' '{"schema_version":1,"status":"FAIL","aggregate_root":null,"inputs":[],"actions":[],"images":[],"runner_labels":[],"runtime_tool_acquisitions":[],"findings":[{"code":"missing_tool","message":"jq is required"}]}'
  exit 2
fi

if command -v shasum >/dev/null 2>&1; then
  hash_file() { shasum -a 256 -- "$1" | awk '{print $1}'; }
elif command -v sha256sum >/dev/null 2>&1; then
  hash_file() { sha256sum -- "$1" | awk '{print $1}'; }
else
  printf '%s\n' '{"schema_version":1,"status":"FAIL","aggregate_root":null,"inputs":[],"actions":[],"images":[],"runner_labels":[],"runtime_tool_acquisitions":[],"findings":[{"code":"missing_tool","message":"shasum or sha256sum is required"}]}'
  exit 2
fi

inputs=()
input_pairs=()
actions=()
images=()
runner_labels=()
runtime_tools=()
findings=()
seen_paths=$'\n'
observation_count=0
failure_count=0

add_finding() {
  local code=$1 message=$2 path=${3:-} line=${4:-0} value=${5:-}
  findings+=("$(jq -cn --arg code "$code" --arg message "$message" --arg path "$path" --arg value "$value" --argjson line "$line" \
    '{code:$code,message:$message,source:(if $path == "" then null else {path:$path,line:$line} end),value:(if $value == "" then null else $value end)}')")
  failure_count=$((failure_count + 1))
}

add_input() {
  local path=$1 file="$repo_root/$1" digest
  if [[ "$seen_paths" == *$'\n'"$path"$'\n'* ]]; then
    add_finding duplicate_input "input path appears more than once" "$path" 0 "$path"
    return
  fi
  seen_paths+="$path"$'\n'
  digest=$(hash_file "$file" 2>/dev/null || true)
  if [[ ! "$digest" =~ ^[0-9a-fA-F]{64}$ ]]; then
    add_finding unreadable_input "input could not be hashed" "$path" 0 "$path"
    return
  fi
  digest=$(printf '%s' "$digest" | tr 'A-F' 'a-f')
  inputs+=("$(jq -cn --arg path "$path" --arg sha256 "$digest" '{path:$path,sha256:$sha256}')")
  input_pairs+=("$path"$'\t'"$digest")
}

validate_input() {
  local path=$1 tracked_count file="$repo_root/$1"
  tracked_count=$(git -C "$repo_root" ls-files -- "$path" | wc -l | tr -d ' ')
  if [[ "$tracked_count" == 0 ]]; then
    add_finding missing_input "required input is not tracked" "$path" 0 "$path"
    return
  fi
  if [[ "$tracked_count" != 1 ]]; then
    add_finding duplicate_input "required input has duplicate tracked entries" "$path" 0 "$path"
    return
  fi
  if [[ -L "$file" ]]; then
    add_finding symlinked_input "input must not be a symlink" "$path" 0 "$path"
    return
  fi
  if [[ ! -f "$file" || ! -r "$file" ]]; then
    add_finding unreadable_input "input is missing or unreadable" "$path" 0 "$path"
    return
  fi
  add_input "$path"
}

add_observation() {
  local kind=$1 classification=$2 value=$3 path=$4 line=$5 status=$6 code=$7 message=$8
  local record
  observation_count=$((observation_count + 1))
  record=$(jq -cn --arg classification "$classification" --arg value "$value" --arg path "$path" --arg status "$status" --argjson line "$line" \
    '{classification:$classification,source:{path:$path,line:$line},status:$status,value:$value}')
  case "$kind" in
    action) actions+=("$record") ;;
    image) images+=("$record") ;;
    runner_label) runner_labels+=("$record") ;;
    runtime_tool) runtime_tools+=("$record") ;;
  esac
  if [[ "$status" == unresolved ]]; then
    add_finding "$code" "$message" "$path" "$line" "$value"
  fi
}

classify_action() {
  local value=$1 path=$2 line=$3 classification status code message
  value=${value#\"}; value=${value%\"}; value=${value#\'}; value=${value%\'}; value=${value%,}
  if [[ "$value" == ./* ]]; then
    classification=local_unhashed; status=unresolved; code=unhashed_local_action; message="local action contents are not included in the input inventory"
  elif [[ "$value" =~ ^[^@[:space:]]+@[0-9a-fA-F]{40}$ ]]; then
    classification=immutable_commit; status=resolved; code=; message=
  elif [[ "$value" == *'${{'* || "$value" != *@* ]]; then
    classification=unclassified; status=unresolved; code=unclassified_action; message="action reference is not content-bound"
  else
    classification=mutable_tag; status=unresolved; code=mutable_action_tag; message="action reference uses a mutable tag or ref"
  fi
  add_observation action "$classification" "$value" "$path" "$line" "$status" "$code" "$message"
}

classify_image() {
  local value=$1 path=$2 line=$3 classification status code message
  value=${value#\"}; value=${value%\"}; value=${value#\'}; value=${value%\'}; value=${value%,}
  if [[ "$value" == scratch ]]; then
    classification=intrinsic; status=resolved; code=; message=
  elif [[ "$value" == *'${{'* || "$value" == *'@sha256:' && ! "$value" =~ @sha256:[0-9a-fA-F]{64}$ ]]; then
    classification=unclassified; status=unresolved; code=unclassified_image; message="image reference is not a valid immutable digest"
  elif [[ "$value" =~ @sha256:[0-9a-fA-F]{64}$ ]]; then
    classification=immutable_digest; status=resolved; code=; message=
  else
    classification=tag_only; status=unresolved; code=tag_only_image; message="image reference is not content-bound"
  fi
  add_observation image "$classification" "$value" "$path" "$line" "$status" "$code" "$message"
}

classify_runner() {
  local value=$1 path=$2 line=$3 classification status code message
  value=${value#\"}; value=${value%\"}; value=${value#\'}; value=${value%\'}; value=${value%,}
  if [[ -z "$value" || "$value" == *'${{'* ]]; then
    classification=unclassified; status=unresolved; code=unclassified_runner_label; message="runner label is not a fixed label"
  elif [[ "$value" == *-latest ]]; then
    classification=mutable_latest; status=unresolved; code=mutable_runner_label; message="runner label uses a mutable latest alias"
  else
    classification=fixed; status=resolved; code=; message=
  fi
  add_observation runner_label "$classification" "$value" "$path" "$line" "$status" "$code" "$message"
}

classify_tool() {
  local value=$1 path=$2 line=$3 classification status code message output digest
  output=
  if [[ "$value" =~ (curl|wget)[^\&\|\;]*(-[A-Za-z]*[oO]|--output)[[:space:]]+([A-Za-z0-9._/-]+) ]]; then
    output=${BASH_REMATCH[3]}
  fi
  digest=$(printf '%s' "$value" | grep -Eo '[0-9a-fA-F]{64}' | head -n 1 || true)
  if [[ -n "$output" && -n "$digest" && "$value" == *"$digest  $output"* ]] &&
     [[ "$value" =~ (sha256sum|shasum)[[:space:]].*(-c|--check) ]]; then
    classification=content_bound; status=resolved; code=; message=
  else
    classification=non_content_bound; status=unresolved; code=non_content_bound_tool; message="runtime tool acquisition is not content-bound"
  fi
  add_observation runtime_tool "$classification" "$value" "$path" "$line" "$status" "$code" "$message"
}

scan_docker_command() {
  local path=$1 line_number=$2 command=$3 token found=0 subcommand=0 skip_next=0 words=()
  read -r -a words <<< "$command"
  for token in "${words[@]}"; do
    token=${token#\"}; token=${token%\"}; token=${token#\'}; token=${token%\'}
    token=${token%,}; token=${token%;}; token=${token%\\}
    if ((found == 0)); then
      [[ "$token" == docker ]] && found=1
      continue
    fi
    if ((subcommand == 0)); then
      case "$token" in run|create|pull) subcommand=1 ;; esac
      continue
    fi
    if ((skip_next)); then skip_next=0; continue; fi
    case "$token" in
      --name|-e|--env|-p|--publish|-v|--volume|--network|--hostname|--user|-w|--workdir|--entrypoint|--platform|--label|--mount)
        skip_next=1
        continue
        ;;
      --*=*|-*) continue ;;
    esac
    classify_image "$token" "$path" "$line_number"
    return
  done
  add_observation image unclassified "<missing>" "$path" "$line_number" unresolved unclassified_image "docker command has no classifiable image reference"
}

scan_line() {
  local path=$1 line_number=$2 line=$3 clean runner_value item runner_items=()
  clean=${line%%#*}
  if [[ "$path" == .github/workflows/*.yml || "$path" == .github/workflows/*.yaml ]]; then
    if [[ "$line" =~ uses:[[:space:]]*([^[:space:]]+) ]]; then
      classify_action "${BASH_REMATCH[1]}" "$path" "$line_number"
    fi
    if [[ "$line" =~ runs-on:[[:space:]]*(.*)$ ]]; then
      runner_value=${BASH_REMATCH[1]%%#*}
      runner_value=${runner_value%$'\r'}
      if [[ "$runner_value" == \[*\] ]]; then
        runner_value=${runner_value#\[}; runner_value=${runner_value%\]}
        IFS=',' read -r -a runner_items <<< "$runner_value"
        for item in "${runner_items[@]}"; do
          item=${item#"${item%%[![:space:]]*}"}; item=${item%"${item##*[![:space:]]}"}
          classify_runner "$item" "$path" "$line_number"
        done
      else
        runner_value=${runner_value#"${runner_value%%[![:space:]]*}"}; runner_value=${runner_value%"${runner_value##*[![:space:]]}"}
        classify_runner "$runner_value" "$path" "$line_number"
      fi
    fi
  fi
  if [[ "$path" == Dockerfile || "$path" == .github/workflows/*.yml || "$path" == .github/workflows/*.yaml ]]; then
    if [[ "$clean" =~ ^[[:space:]]*FROM[[:space:]]+([^[:space:]]+) ]]; then
      classify_image "${BASH_REMATCH[1]}" "$path" "$line_number"
    fi
    if [[ "$clean" == *docker\ run* || "$clean" == *docker\ create* || "$clean" == *docker\ pull* ]]; then
      scan_docker_command "$path" "$line_number" "$clean"
    fi
  fi
  if [[ "$clean" =~ (^|[[:space:]])(go[[:space:]]+install|sudo[[:space:]]+apt(-get)?[[:space:]]+install|apt(-get)?[[:space:]]+install|apk[[:space:]]+add|npm[[:space:]]+(install|i)|pnpm[[:space:]]+add|yarn[[:space:]]+add|pip3?[[:space:]]+install|cargo[[:space:]]+install|brew[[:space:]]+install)([[:space:]]|$) ]]; then
    add_observation runtime_tool non_content_bound "$clean" "$path" "$line_number" unresolved non_content_bound_tool "runtime package installation is not content-bound by this repository"
  elif [[ "$clean" =~ (^|[[:space:]])(curl|wget)[[:space:]] ]] && [[ "$clean" != *'/dev/null'* ]]; then
    classify_tool "$clean" "$path" "$line_number"
  elif [[ "$path" == .github/workflows/*.yml || "$path" == .github/workflows/*.yaml ]] &&
       [[ "$clean" =~ run:.*(^|[[:space:]])(fetch|acquire|bootstrap|download)[[:space:]] ]]; then
    add_observation runtime_tool unclassified "$clean" "$path" "$line_number" unresolved unclassified_acquisition "acquisition syntax is not classified"
  fi
}

workflow_paths=()
while IFS= read -r -d '' path; do
  case "$path" in
    .github/workflows/*.yml|.github/workflows/*.yaml) workflow_paths+=("$path") ;;
    .github/workflows/*) add_finding unexpected_input "tracked workflow input has an unexpected extension" "$path" 0 "$path" ;;
  esac
done < <(git -C "$repo_root" ls-files -z -- .github/workflows)

if ((${#workflow_paths[@]} == 0)); then
  add_finding empty_scan "no tracked workflow inputs were found" .github/workflows 0 .github/workflows
fi

required_paths=(
  Dockerfile Makefile go.mod go.sum scripts/quality-generated.sh
  internal/protocol/schema/protocol.schema.json
  cmd/schemagen/main.go internal/protocol/protocol.go
  internal/protocol/generated.go internal/protocol/protocol.d.ts
)
for path in "${required_paths[@]}"; do validate_input "$path"; done
if ((${#workflow_paths[@]})); then
  for path in "${workflow_paths[@]}"; do validate_input "$path"; done
fi

scan_file() {
  local path=$1 line_number=0 line logical= logical_start=0
  [[ -f "$repo_root/$path" && -r "$repo_root/$path" && ! -L "$repo_root/$path" ]] || return 0
  while IFS= read -r line || [[ -n "$line" ]]; do
    line_number=$((line_number + 1))
    if [[ -n "$logical" ]]; then
      logical+=" ${line%\\}"
      if [[ "$line" == *\\ ]]; then continue; fi
      scan_line "$path" "$logical_start" "$logical"
      logical=
    elif [[ "$line" =~ docker[[:space:]]+(run|create|pull)([[:space:]]|$) && "$line" == *\\ ]]; then
      logical=${line%\\}
      logical_start=$line_number
    else
      scan_line "$path" "$line_number" "$line"
    fi
  done < "$repo_root/$path"
  [[ -z "$logical" ]] || scan_line "$path" "$logical_start" "$logical"
}
if ((${#workflow_paths[@]})); then
  for path in "${workflow_paths[@]}"; do scan_file "$path"; done
fi
scan_file Dockerfile
scan_file Makefile
scan_file scripts/quality-generated.sh
if ((observation_count == 0)); then add_finding empty_scan "no acquisition declarations were found" . 0 ""; fi

sorted_pairs=$(printf '%s\n' "${input_pairs[@]}" | LC_ALL=C sort)
aggregate_root=$(printf '%s\n' "$sorted_pairs" | hash_file /dev/stdin | tr 'A-F' 'a-f')

json_array() {
  if (($# == 0)); then
    printf '[]'
  else
    printf '%s\n' "$@" | jq -cs 'sort_by((.source.path // .path // .code), (.source.line // 0), (.value // ""))'
  fi
}

inputs_json=$(if ((${#inputs[@]})); then json_array "${inputs[@]}" | jq -c 'sort_by(.path)'; else printf '[]'; fi)
actions_json=$(if ((${#actions[@]})); then json_array "${actions[@]}"; else printf '[]'; fi)
images_json=$(if ((${#images[@]})); then json_array "${images[@]}"; else printf '[]'; fi)
runner_json=$(if ((${#runner_labels[@]})); then json_array "${runner_labels[@]}"; else printf '[]'; fi)
runtime_json=$(if ((${#runtime_tools[@]})); then json_array "${runtime_tools[@]}"; else printf '[]'; fi)
findings_json=$(if ((${#findings[@]})); then json_array "${findings[@]}"; else printf '[]'; fi)
status=PASS
((failure_count == 0)) || status=FAIL

jq -S -c -n \
  --arg status "$status" --arg aggregate_root "$aggregate_root" \
  --argjson inputs "$inputs_json" --argjson actions "$actions_json" --argjson images "$images_json" \
  --argjson runner_labels "$runner_json" --argjson runtime_tool_acquisitions "$runtime_json" --argjson findings "$findings_json" \
  '{schema_version:1,status:$status,aggregate_root:$aggregate_root,inputs:$inputs,actions:$actions,images:$images,runner_labels:$runner_labels,runtime_tool_acquisitions:$runtime_tool_acquisitions,findings:$findings}'

[[ "$status" == PASS ]] && exit 0
exit 1
