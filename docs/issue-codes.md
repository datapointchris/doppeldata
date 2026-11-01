# Issue codes

Every refusal doppeldata raises holds one or more issues, and each issue names a registered code.
A caller branches on the code, never on the message, so a message can be reworded without
breaking anyone. `doppeldata capabilities show --json` lists every code with its exit status,
what raises it and its hint.

## A code is `E-<AREA>-<NNN>`, and its area names what was refused

| Area | What was refused |
| --- | --- |
| `PROFILE` | a profile or bundle document, a column it describes, or the profile named |
| `SCENARIO` | a scenario document, or what it asks of the profile and the files |
| `POLICY` | a privacy rule a profile breaks, or the policy and `k_min` named |
| `DATA` | the data read to profile or check, or what a check found in it |
| `WRITE` | a write's destination, layout or files |
| `IO` | a store or local file that cannot be reached or read |
| `IMPORT` | a schema a seed is imported from |
| `EXPORT` | a format or table an export was asked for |
| `USAGE` | an argument or a command line |

- **The number only decides where a code is allocated.** Codes declared in Python start at 001,
  the engine's at 101 and the writers' at 201, so two declarers never take one number. A check
  that moves to another declarer keeps its code.
- **A code never changes meaning.** A caller that branched on it last release branches on it
  this one. A refusal reworded keeps its code, and a new refusal takes a new one.

## Every code is declared once, with its exit status and hint

- **The engine's and the writers' codes are declared in Rust.** The `issue_codes!` macro in
  `crates/core/src/issue_codes.rs` and `crates/writers/src/issue_codes.rs` declares each one, and
  every fault enum's `code()` returns one. So a fault the engine adds cannot compile without its
  code.
- **The extension's `issue_codes()` hands them to Python.** `doppeldata.issue_codes` loads them
  beside the codes Python declares, and binds each to a constant of the same name.
- **`declare` refuses a malformed registry when the module is imported.** A code of another form,
  an area not listed, an exit status outside 1 to 4, or a code declared twice raises TypeError.
- **`Issue` refuses an unregistered code when it is built**, and takes the code's hint where the
  raiser gives none.
- **`cli/tests/test_issue_codes.py` refuses a source that spells a code**, or builds a refusal from
  a string literal. So every code a refusal carries comes through the registry.

## The exit status belongs to the code

| Exit | Meaning |
| --- | --- |
| 1 | The work was attempted and failed: a write stopped, the data changed while it was read, or two profiles drifted |
| 2 | The call must change. Nothing was written. |
| 3 | A check found what a person must act on: data missed its profile, or a profile breaks its policy |
| 4 | A store or service never answered |

- **`exit_code` is read from the first issue's code.** An error holding several issues ends on
  the first one's status.
- **A check verb ends on 3 where something it wraps fails with 1.** So `profile diff` keeps 1 for
  drift and nothing else, and `data check` ends on 3 whenever it does not pass.
- **A profile `data generate` refuses because it fails `profile check` exits 2.** `profile check`
  itself exits 3 on the same profile, because there the check is the command.

## An issue says where, what was given, what is taken, and what to do

| Key | Holds |
| --- | --- |
| `code` | the registered code |
| `message` | what was found against what was expected |
| `field` | an RFC 6901 pointer into the document refused, `""` for its root, or the argument or option refused as the call spells it, and null where it names neither |
| `valid` | every value the field takes, where they can be listed |
| `hint` | what to run or change next |
| `value` | what the field was given |
| `closest` | the values of `valid` nearest `value`, nearest first, at most three |

- **An error holds its issues as `.issues`.** `.code` and `.message` are the first's, and
  `str(error)` starts with `<code>: <message>`. An error of several issues has a headline, and its
  text lists each issue at its field.
- **A profile check's violations are issues too.** Each adds the `rule` a literal broke, or the
  `treatment` whose rule a column broke.

## The command line prints a refusal as text, or as one JSON line per issue

```text
$ doppeldata data plan ordres --out out/
doppeldata: E-PROFILE-028: ordres is neither a profile file nor an example; name a file, or an example
  closest: orders
  valid:
    claims
    eight-columns
    events-json
    orders
  hint: doppeldata profile list names every example
```

With `--json` anywhere before a `--`, each issue prints on stderr as one line, every key present:

```json
{"error": {"code": "E-PROFILE-028", "message": "ordres is neither a profile file nor an example; name a file, or an example", "field": "PROFILE", "valid": ["claims", "eight-columns", "events-json", "orders"], "hint": "doppeldata profile list names every example", "value": "ordres", "closest": ["orders"]}}
```

- **The root group reports every refusal**, so a new verb needs no handler of its own.
- **A parameter refused for what the library refused reports the library's issues.** Each takes
  the parameter as its field, as `PROFILE` above, unless it already points into a document.
- **A command line the parser refuses is `E-USAGE-001` under `--json`.** Its issue names the
  option, every option the command takes, the nearest of them, and `<command> --help` as its hint.
  Without `--json` the parser's own usage text prints, and it exits 2 either way.
- **A file the parser could not read is `E-IO-003`**, and a file `--out` cannot be written to is
  `E-WRITE-012`.

## Rejected

- *Rejected:* a slug of words for each code, such as `unknown-field`. One slug named an unknown key
  in a profile and in a scenario alike, so a caller branching on it could not tell which document
  was refused. A slug is also a phrase, and rewording a refusal pulls its slug along, breaking
  every caller matching it.
- *Rejected:* an exception class per code. The engine's fault enums already give each variant its
  code, and a class per code would restate every variant in Python. A caller catches the class it
  can act on, such as `InvalidProfile` or `WriteFailed`, and branches on `.code` within it.
