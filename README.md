# The Hat Backup System


Disclaimer: This is not an official Google product.

Warning: This is an incomplete work-in-progress.

Warning: This project does currently NOT support security or privacy.


## Status
This software is pre-alpha and should be considered severely unstable.

This software should not be considered ready for any use; the code is currently
provided for development and experimentation purposes only.


## Project
The goal of hat is to provide a backend-agnostic snapshotting backup system,
complete with deduplication of file blocks and efficient navigation of backed up
files.

A sub-goal is to do so in a safe and fault-tolerant manner, where a process
crash is followed by quick and safe recovery.

Further, we aim for readable and maintainable code, partly by splitting the
system into a few sub-systems with clear responsibility.


Disclaimer: The above text describes our goal and not the current status.


## Building from source
0. Install rust 0.12.0 and newest Cargo
   * Rust nighly is available from http://rust-lang.org
   * Path for Cargo nightlies can be found at https://github.com/rust-lang/cargo
     * Currently x86\_64 is https://static.rust-lang.org/cargo-dist/cargo-nightly-x86_64-unknown-linux-gnu.tar.gz
1. Checkout the newest version of the source:
   * `git clone https://github.com/google/hat-backup.git`
   * `cd hat`
2. Let Cargo build everything needed:
   * `cargo build`

## Try the hat executable using Cargo (the binary is in target/)
   * `cargo run snapshot my_snapshot /some/path/to/dir`
   * `cargo run checkout my_snapshot output/dir`

## Generate source code documentation:
   * `cargo doc`
   * `${BROWSER} target/doc/hat-lib/index.html`


## License and copyright
See the files LICENSE and AUTHORS.


## Contributions
We gladly accept contributions/fixes/improvements etc. via GitHub pull requests
or any other reasonable means, as long as the author has signed the Google
Contributor License.

The Contributor License exists in two versions, one for individuals and one for
corporations:

https://developers.google.com/open-source/cla/individual
https://developers.google.com/open-source/cla/corporate


Please read and sign one of the above versions of the Contributor License,
before sending your contribution. Thanks!


## Authors
See the AUTHORS.txt file.

This project is inspired by a previous version of the system written in Haskell:
https://github.com/mortenbp/hindsight
