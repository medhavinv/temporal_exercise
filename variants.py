"""Selectable behaviours for the order saga, so you never have to edit code.

WHY THIS FILE EXISTS
--------------------
Teaching repos usually say "uncomment line 62 and re-run". That's annoying, and
worse, it means a Worker can end up running code that no longer matches the file
on disk. Instead every variation lives here behind a name, and you pick one at
the command line:

    python starter.py start --variant no-retries
    ./lab.sh variant no-retries

HOW THE VARIANT REACHES THE WORKFLOW -- and why it matters
----------------------------------------------------------
The variant name travels as part of the Workflow's *input*, not as an
environment variable read inside the Workflow.

That is not an arbitrary choice. Workflow code is re-executed on every replay,
so anything it reads must produce the same answer every time. An environment
variable fails that test: restart the Worker with a different value, or run two
Workers with different settings, and a replay would take a different branch than
the original execution -- a non-determinism error, or worse, silent corruption.

Workflow *input* is recorded in the Event History at
`WorkflowExecutionStarted`. Replaying reads the same recorded bytes forever, so
branching on it is safe. The same reasoning is why `workflow.now()` exists
instead of `datetime.now()`.

Activity-side behaviour (injected failures) is different: Activities have no
determinism rules, so those stay as Worker flags -- see `worker.py --help`.
"""

from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True)
class Variant:
    """One selectable behaviour of OrderWorkflow.

    Every field is read by `workflows.py` at the point it matters. Because the
    whole object is derived from the recorded variant *name*, replay always
    reconstructs identical values.
    """

    name: str
    summary: str

    # How long the customer has to cancel before we ship. A durable Timer, so
    # the length costs nothing to wait out -- 30 days is as cheap as 10 seconds.
    cancellation_window: timedelta = timedelta(seconds=10)

    # Attempts per Activity before the failure reaches Workflow code. 1 means
    # "no retries": the first failure is final.
    max_attempts: int = 5

    # Whether a shipping failure unwinds the completed steps. Turning this off
    # shows what a saga protects you from.
    compensate_on_failure: bool = True

    # Deliberately call datetime.now() inside Workflow code. The Python SDK's
    # sandbox rejects it, which is the point -- see the `broken-determinism`
    # variant below.
    break_determinism: bool = False


VARIANTS: dict[str, Variant] = {
    "default": Variant(
        name="default",
        summary="The saga as designed: 10s cancellation window, 5 attempts, "
        "compensation on failure.",
    ),
    "long-window": Variant(
        name="long-window",
        summary="A 30-day cancellation window. Proves a durable Timer costs "
        "nothing to hold, and that time-skipping tests do not care.",
        cancellation_window=timedelta(days=30),
    ),
    "no-retries": Variant(
        name="no-retries",
        summary="max_attempts=1, so the first Activity failure is final. "
        "Compare the history against the default's retry behaviour.",
        max_attempts=1,
    ),
    "no-compensation": Variant(
        name="no-compensation",
        summary="Skip the saga rollback on shipping failure, leaving the "
        "customer charged for goods that never ship.",
        compensate_on_failure=False,
    ),
    "broken-determinism": Variant(
        name="broken-determinism",
        summary="Calls datetime.now() inside Workflow code. The sandbox "
        "rejects it and the Workflow Task fails on a loop.",
        break_determinism=True,
    ),
}

DEFAULT = "default"


def get(name: str) -> Variant:
    """Look up a variant, failing loudly on a typo rather than silently."""
    try:
        return VARIANTS[name]
    except KeyError:
        raise ValueError(
            f"unknown variant {name!r}. Available: {', '.join(sorted(VARIANTS))}"
        ) from None


def describe_all() -> str:
    """Human-readable listing, used by --list-variants and ./lab.sh variants."""
    width = max(len(n) for n in VARIANTS)
    lines = []
    for name in sorted(VARIANTS):
        variant = VARIANTS[name]
        # Wrap the summary under the name so long descriptions stay readable.
        words, line = variant.summary.split(), ""
        wrapped = []
        for word in words:
            if len(line) + len(word) + 1 > 66:
                wrapped.append(line)
                line = word
            else:
                line = f"{line} {word}".strip()
        wrapped.append(line)
        lines.append(f"  {name:<{width}}  {wrapped[0]}")
        lines.extend(f"  {'':<{width}}  {rest}" for rest in wrapped[1:])
    return "\n".join(lines)
