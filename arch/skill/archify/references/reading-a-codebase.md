# Reading a real codebase so the diagram does not lie

This reference exists because of a specific, repeatable failure: a diagram of a real
repository looked finished, passed every mechanical gate, and was still **wrong in its
central claim** — it drew one implementation as if it were the architecture, while the
code had a strategy port with two implementations behind it.

Every check in this file is about *what the picture asserts*, not about layout.

## The failure this prevents

A dispatcher module contained a `Selector` trait, a `P2CSelector` (≈300 lines of
scoring), and a `WeightedRandom` (≈40 lines). Reading the file top to bottom, the P2C
implementation looks like the design: it is long, documented, and named after the
algorithm everyone talks about. So the diagram became "P2C decides the route", with a
node literally labelled `P2CSelector` in the decision slot.

The code actually had:

- a `SelectorKind` config enum (`P2c | WeightedRandom`) in the vocabulary crate,
- a `SelectorSlot` enum holding either the built-in P2C or `Box<dyn Selector>`,
- a `wire_signals` match that installs one of them at boot,
- a `with_selector` extension point for consumer strategies,
- and a **second decision layer above the strategy** (priority tiers, rate-limit
  re-draw) that the diagram omitted entirely.

None of those facts are visible from the implementation file. All of them are one
`callers` query away.

## Rule 1 — enumerate the port before you draw its implementation

Any node that represents "the thing that decides / renders / stores / sends" is almost
always behind a trait. Before placing it, resolve four things:

1. **the port** — the trait, its exact method signature, and what the return type implies;
2. **every implementation** in production code (not tests, not benches, not examples);
3. **the selection point** — the `match`, the `if`, or the builder call that chooses one;
4. **the extension point** — whether a consumer can inject their own, and what contract
   that carries.

If you cannot name all four, you are drawing a guess.

### Concrete procedure

Port census over production code only:

```bash
# every trait impl for a port, excluding tests/benches/examples
for t in Selector Egress PriceTable Stage Gate Ledger; do
  echo "## $t"
  grep -rn "impl.*\b$t for " --include=*.rs crates/ src/ \
    | grep -vE '/(tests|benches|examples)/'
done
```

Then resolve the *selection* side, which is where the interesting design lives:

```bash
codegraph callers with_selector     # who installs a strategy, and from where
codegraph callers DispatchStage     # is this stage actually wired, or test-only?
codegraph query SelectorKind        # the config enum and its doc contract
```

`callers` on a constructor or a setter is the highest-value query in this whole
workflow: it finds the assembly point that a file-by-file read never reaches.

### What "test-only" looks like

A type can implement the port, be exported from the crate root, be exercised by tests,
and still never appear in the assembled product. In this codebase two `Stage`
implementations are in exactly that state. Draw them — but draw them as *not wired*,
with a dashed edge and a label that says so, or leave them out and put them in a card.
Do not place them on the main path because they exist.

Distinguish three states explicitly:

| state | evidence | how to draw |
|---|---|---|
| wired | reachable from the assembly function | solid edge on the path |
| optional | behind a config flag or `Option` mount | dashed edge, name the flag |
| test-only / unused | only referenced from `tests/`, `benches/`, `examples/` | outside the path, labelled, or in a card |

## Rule 2 — the constant that selects a strategy is part of the diagram

If a `match` chooses between implementations, the enum being matched is a node in the
architecture, not an implementation detail. Draw the flow
`config enum → assembly point → slot → implementations`.

The reason is reader-facing: someone asking "can I plug in my own policy?" can only
answer it by seeing the enum and the injection point. A diagram that starts at the
implementation answers a question nobody asked.

## Rule 3 — look for a second decision layer above the port

Ports rarely sit at the top. In the case above, two filters ran *before* the strategy
was consulted (priority tiers, then rate-limit refusal re-draw), and one ran *after*
(health accounting). A diagram of the strategy alone would suggest the strategy sees the
whole candidate pool; it only ever sees the eligible-and-highest-tier slice.

Before drawing a decision node, ask: what has already been filtered out by the time this
code runs? Then check whether the caller filters again between rounds.

## Rule 4 — one implementation is a claim, two is an architecture

If the census returns exactly one production implementation, say so in the card
("single implementation today; the port exists for X"). If it returns two or more, both
must appear, even if one is only used by the standalone binary rather than the library.

Two near-misses caught by exactly this check in the same review:

- a price-table port with a library implementation and a **separate implementation in
  the daemon's catalog loader** — the diagram showed only the library one;
- an egress port with a `reqwest` implementation and a **protocol-dialer implementation**
  for the transports reqwest cannot carry — the diagram showed only `reqwest`.

Neither was caught by validation, layout checks, or the browser gate. Mechanical gates
verify that the picture is well-formed, never that it is true.

## Rule 5 — a passing receipt is not evidence of correctness

`finalize` proves: schema-valid, renderable, no overlapping geometry, readable at
desktop width, sources resolve to real paths and in-range lines. It proves nothing about
whether a node should exist.

So the review order is:

1. semantic pass (this file) — does every node and edge correspond to a fact I can cite?
2. authoring pass — placement, labels, routes;
3. `finalize` — mechanical gates;
4. perceptual pass — open the capture and read it as a stranger.

Doing step 3 first produces a beautiful wrong diagram, which is worse than an ugly
correct one because it survives review.

## Rule 6 — write the census down while you work

Keep a short table in the working folder while authoring:

```
| port | impls | assembly point | in diagram? |
```

Fill it from the commands above, not from memory. Two reasons: it makes the omission
visible before drawing, and it becomes the review artifact when someone asks "how do you
know there are only two?". Hand it over with the diagram.

## Self-check before `finalize`

Answer all of these; any "no" means the diagram is not ready:

- [ ] Every trait-like node names its port and I can list **all** production impls.
- [ ] Every "chooses between implementations" site has the selecting enum/config visible.
- [ ] I know which nodes are wired, optional, and test-only — and the drawing says which.
- [ ] I checked what filters run before and after each decision node.
- [ ] Every source range was read, not inferred from a filename or a module doc.
- [ ] No node exists only because a type with that name exists.
- [ ] The card text states single-implementation ports as such.

## Recipe: an at-a-glance crate map

When the request is "let me see at a glance what each crate does", one card per
package, five facts, all mechanically derivable:

| field | how to get it | why the reader wants it |
|---|---|---|
| size | `find <crate>/src -name '*.rs' | xargs wc -l` | effort vs. surface area |
| one-line duty | the crate's `//!` header, read in full | the author's own claim, not your summary |
| core types | `grep -rhoE "^pub (struct|trait|enum) [A-Za-z0-9_]+" <crate>/src` | the vocabulary you will actually type |
| entry points | `codegraph query` on the crate's main type, then `callees` | where execution starts |
| file map | `ls <crate>/src` | which file to open first |

Mark traits with `{trait}` in the card so a reader can tell a port from a value, and
strip the shared `gateway-` prefix from the card title while keeping the full name in
the dependency table — the prefix is noise repeated twelve times, but the full name is
what appears in `Cargo.toml`.

Order the cards by dependency layer, not alphabetically or by size: the layer is the
information, the card is the detail. Keep the lateral-edge table separate from the
cards; putting the exceptions next to the nodes is what makes a layered picture
unreadable.
