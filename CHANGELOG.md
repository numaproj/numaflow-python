# Changelog

## v0.4.0 (2023-04-14)

 * [58ff85c](https://github.com/numaproj/numaflow-python/commit/58ff85caad698342aaaf4b71d19720d13251de1c) chore!: remove support for py3.8; add 3.11 (#85)
 * [405c5cd](https://github.com/numaproj/numaflow-python/commit/405c5cd01023bc71370058bca2ab0734aec6b2e5) doc: explain how to use datum metadata. (#78)
 * [d252283](https://github.com/numaproj/numaflow-python/commit/d2522835a3db06aea536e90397da4bb8f4740ba8) feat: introducing tags for conditional forwarding (#76)
 * [786b48e](https://github.com/numaproj/numaflow-python/commit/786b48e06039a238820b29f711f416c9fe0e229a) feat: Add ID and numDelivered fields to Datum in udf (#77)
 * [1849f10](https://github.com/numaproj/numaflow-python/commit/1849f103c3fe53b8c0dd814e3335784efe0ad344) fix: doc typo fix (#75)
 * [d142648](https://github.com/numaproj/numaflow-python/commit/d142648f7cbeb3b816d7b30267bf3a0f4d5c4366) feat: add multiprocessing support for Map  (#73)
 * [acba109](https://github.com/numaproj/numaflow-python/commit/acba1090060a6cffc0071d4d9ff8bb659b225eba) feat: support for multi keys (#72)
 * [6fa43d6](https://github.com/numaproj/numaflow-python/commit/6fa43d6b620daf3f8661bfd732648a0ccd839a0c) fix: stream to stdout stderr based on log level (#69)
 * [f6c914e](https://github.com/numaproj/numaflow-python/commit/f6c914e056b2bb4d3f1bd97888ab44b5fe2055c2) fix: store strong references of task objects  (#66)
 * [3e81304](https://github.com/numaproj/numaflow-python/commit/3e81304e03d15c371e34ff2d188d35d6f0a80bac) refactor: make default constructor private for Message/MessageT (#67)
 * [cd8eac2](https://github.com/numaproj/numaflow-python/commit/cd8eac2a9f6d79f078c81b34948819cc1414d52b) feat: add mapt sdk support (#64)
 * [ea78057](https://github.com/numaproj/numaflow-python/commit/ea78057f02872976ba815280aba6d7f3373919c3) feat: bidirectional streaming for reduce (#55)

### Contributors

 * Avik Basu
 * Keran Yang
 * San Nguyen
 * Sidhant Kohli
 * Vigith Maurice
 * Yashash H L
 * xdevxy

## v0.3.3 (2023-02-14)

 * [f13bc5f](https://github.com/numaproj/numaflow-python/commit/f13bc5fd124c7c8141e37d93399e013771af09ea) feat: reduce stream per window using asyncio (#49)
 * [df9c8c8](https://github.com/numaproj/numaflow-python/commit/df9c8c89d9bfa5f7f934c1f8132cb202bc27e6e2) perf: improve performance by adding slots (#47)

### Contributors

 * Avik Basu
 * ashwinidulams

## v0.3.2 (2022-12-21)

 * [a650aac](https://github.com/numaproj/numaflow-python/commit/a650aacf86b5d18f8f84ba75be2b9adf3e6dacdc) fix: time unit bug/dockerfile/add a guide (#44)

### Contributors

 * Juanlu Yu

## v0.3.1 (2022-12-20)

 * [d33ddd0](https://github.com/numaproj/numaflow-python/commit/d33ddd0ccb6725b711c16f8a744e027767c26c1d) feat: udf reduce (#40)

### Contributors

 * Juanlu Yu

## v0.3.0 (2022-12-15)

 * [e717b1b](https://github.com/numaproj/numaflow-python/commit/e717b1ba5ecda7fde218177d2f78fa550cd8305f) feat: udsink gRPC stream (#34)
 * [ad55710](https://github.com/numaproj/numaflow-python/commit/ad5571052b3857377d7b920c6fe0bf89fdb76c58) fix: .flake8 file (#35)

### Contributors

 * Avik Basu
 * Juanlu Yu

## v0.2.6 (2022-11-22)

 * [0fbecc1](https://github.com/numaproj/numaflow-python/commit/0fbecc13a2ae48636d5454664b072534b02160a0) feat: introduces sync gRPC server  (#27)
 * [b6a4bc0](https://github.com/numaproj/numaflow-python/commit/b6a4bc009035d52dd177079808c708b925fcfc3f) feat: add support to configure max message size (#23)

### Contributors

 * Avik Basu
 * Juanlu Yu

## v0.2.5 (2022-10-31)

 * [97b410e](https://github.com/numaproj/numaflow-python/commit/97b410ecc09a52ca668e1c265f4b73822151ba49) fix: catch exception in udf and udsink (#18)

### Contributors

 * Avik Basu

## v0.2.4 (2022-09-29)

 * [3559e15](https://github.com/numaproj/numaflow-python/commit/3559e15622e0cbb7d16140f7bba5101791f323bf) update function socket (#12)

### Contributors

 * shrivardhan

## v0.2.3 (2022-09-30)

 * [dd861c0](https://github.com/numaproj/numaflow-python/commit/dd861c060bdd0c103a00b94e1544569cf46f9591) doc: update UDSink (#10)

### Contributors

 * Vigith Maurice

## v0.2.2 (2022-09-21)

 * [fd88178](https://github.com/numaproj/numaflow-python/commit/fd881788667bbf99b8592b5d6b29df7426697f58) fix: unix domain socket path and examples (#6)

### Contributors

 * Juanlu Yu

## v0.2.1 (2022-09-19)

 * [043e109](https://github.com/numaproj/numaflow-python/commit/043e1091f65edf6eecef6acc63b079294db5f2c1) feat: udsink gRPC (#5)

### Contributors

 * Juanlu Yu

## v0.2.0 (2022-09-16)

 * [496543c](https://github.com/numaproj/numaflow-python/commit/496543c787eb447d8c28b5ee881669a01e0bdab4) feature: gRPC (#4)

### Contributors

 * Chrome

