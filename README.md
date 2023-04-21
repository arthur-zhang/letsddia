# Let's implement DDIA in rust and golang (with some Java and Cpp).

This repository contains code implementations for 'Designing Data-Intensive Applications (DDIA)' by Martin Kleppmann in
Rust, Go, and Java. The project aims to provide practical examples and hands-on exercises based on the concepts and
principles discussed in the book, showcasing solutions in multiple programming languages.

It serves as a valuable companion for those who want to delve deeper into the world of designing and building scalable
and maintainable data-intensive applications across different language ecosystems.

## project status

| project             | status        | info                                                                 |
|---------------------|---------------|----------------------------------------------------------------------|
| tiny-cask           | rust ✅, go ❌  | currently no multi-thread supported                                  |
| tiny-delay-queue    | partial  done | rust impl according to beanstalkd, based on tokio and priority queue |
| tiny-btree          | rust soon     |                                                                      |
| tiny-lsm            | working hard  |                                                                      |
| tiny-gossip         | working  hard |                                                                      |                                                                      |
| tiny-raft           | working  hard |                                                                      |                                                                      |
| tiny-binlog         | working  hard |                                                                      |                                                                      |
| tiny-ntp            | working  hard |                                                                      |                                                                      |
| tiny-column-storage | working  hard |                                                                      |                                                                      |
| coming soon         | ...           | ..                                                                   |

If you are interested in this project or would like me to prioritize implementing a certain component, please don't
hesitate to submit an issue. If you encounter any problems with the code, you are also welcome to submit a PR with the
appropriate modifications.
