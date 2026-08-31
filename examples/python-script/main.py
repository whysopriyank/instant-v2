"""Hello world InstantDB script — v2 self-host replay of v1's
examples/python-script. Only delta from the frozen example: `api_uri` comes
from INSTANT_API_URI so the frozen PyPI SDK can point at a local instantd.
"""

import os
import time

from dotenv import load_dotenv
from instantdb import Instant, id

load_dotenv()

API_URI = os.environ.get("INSTANT_API_URI", "https://api.instantdb.com")
db = Instant(api_uri=API_URI)


def main() -> None:
    stamp = int(time.time() * 1000)
    todo_id = id()
    db.transact(
        db.tx.todos[todo_id].update(
            {
                "text": f"Hello from Python! ({stamp})",
                "done": False,
                "createdAt": stamp,
            }
        )
    )

    result = db.query({"todos": {}})
    print(f"Found {len(result['todos'])} todo(s):")
    for todo in result["todos"]:
        status = "x" if todo.get("done") else " "
        print(f"  [{status}] {todo['text']}")

    # Round two: merge the existing entity and query the updated value.
    db.transact(db.tx.todos[todo_id].merge({"done": True}))
    result = db.query({"todos": {"$": {"where": {"createdAt": stamp}}}})
    assert len(result["todos"]) == 1 and result["todos"][0]["done"] is True, result
    print("merge+query round-trip OK")


if __name__ == "__main__":
    main()
