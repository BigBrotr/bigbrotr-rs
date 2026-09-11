# Research and revision process

Each revision changes the current design documents; this log records what new
evidence caused the change. The old documents are historical sources, not rules
for the new repository. Six revisions are requested by the owner, not a rule that
production quality can be proved by counting reviews.

| Revision | Main investigation | Expected design consequence | State |
|---|---|---|---|
| 1 | Recover vision, original phases, service responsibilities, and product gaps | Requirements, historical crosswalk, scope and questions | Completed; see revision 1 |
| 2 | Define service contracts, configuration, ownership and capabilities | Complete service catalog and lifecycle model | Completed; see revision 2 |
| 3 | Challenge storage, protocol and Rust library choices with practical probes | Data model, durability protocol and dependency decisions | Completed; see revision 3 |
| 4 | Examine failure, security, resource exhaustion and recovery | Operational contracts and fault-validation programme | Completed; see revision 4 |
| 5 | Exercise installation, dashboard, website, documentation and contributor journeys | Complete product delivery and maintenance plan | Completed; see revision 5 |
| 6 | Reconcile all contracts, evidence, decisions and implementation dependencies | Implementable roadmap and final outstanding risks | In progress |

Research experiments may overlap document revisions when dependencies allow.
Only experiments run and inspected count as evidence. Readiness remains scoped:
an archive prototype does not certify the future collector, and a plan does not
establish production capacity.

Downloaded references, source snapshots and build material remain in the ignored
`.research/` directory. Tracked source manifests identify what was actually read.
Reports retain commands, versions, fixture definitions, limits, outcomes and
limitations sufficient to reproduce the relevant observation.
