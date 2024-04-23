# Changelog

## v0.7.0a1 (2024-04-17)

 * [0c2501b](https://github.com/numaproj/numaflow-python/commit/0c2501b5c3677eb736539a6797bfabd1c02259aa) feat: add fallback sink support (#157)
 * [af095a1](https://github.com/numaproj/numaflow-python/commit/af095a1cf31a0da5841d07fee31fb0f1a5d58e47) fix: convert headers to dict before exposing to users (#151)

### Contributors

 * Sidhant Kohli
 * Yashash H L

## v0.7.0a0 (2024-03-26)

 * [5d99fb8](https://github.com/numaproj/numaflow-python/commit/5d99fb8e4639b4aafdcccb88148e3fd689c6aee8) feat: support headers for message (#138)
 * [04914fa](https://github.com/numaproj/numaflow-python/commit/04914faeda80c8eea9652e4d0de5695ab175faac) fix: readme (#143)
 * [b366e2f](https://github.com/numaproj/numaflow-python/commit/b366e2f82c74a76d7e8542119eb48e594cb7ee6e) feat: implement reduce streaming (#134)
 * [c3f9c60](https://github.com/numaproj/numaflow-python/commit/c3f9c60a4918bb4bd0e37a8d2970e567bd3debcb)  feat: implement update script #102  (#137)
 * [df6afe6](https://github.com/numaproj/numaflow-python/commit/df6afe6a178ff8172757ffd4d612f2965801378d) feat: add support for new reduce proto (#133)
 * [39ec103](https://github.com/numaproj/numaflow-python/commit/39ec103b7a63191c48fa1a0c2f96cbe5db3d270e) feat: class based refactor for SDK (#129)

### Contributors

 * Abdullah Yildirim
 * Sidhant Kohli
 * Vigith Maurice
 * Yashash H L

## v0.6.0 (2023-12-18)

 * [e6e1261](https://github.com/numaproj/numaflow-python/commit/e6e1261881104ddb5b4aac5868745f68c6d2763d) feat: adding partitions for user defined source (#124)
 * [3e3b25f](https://github.com/numaproj/numaflow-python/commit/3e3b25f4096ae0aca5744da0354615eb63e972e6) fix: require event time when dropping messages (#123)

### Contributors

 * Keran Yang
 * Sidhant Kohli

## v0.5.4 (2023-11-21)

 * [2f0d4c4](https://github.com/numaproj/numaflow-python/commit/2f0d4c40a229bfdb4325de968c95768c1ca69827) fix: async source init  (#120)
 * [b24f87d](https://github.com/numaproj/numaflow-python/commit/b24f87d6946379747eaaf182c194be55bf063c7d) fix: typo in async_server.py (#118)

### Contributors

 * Aaron Tolman
 * Sidhant Kohli

## v0.5.3 (2023-10-13)

 * [71c2b52](https://github.com/numaproj/numaflow-python/commit/71c2b52495a994929ad6c950e61b74f4fd622f40) feat: Add User Defined Source support (#114)

### Contributors

 * Sidhant Kohli

## v0.5.2 (2023-09-29)

 * [c04a279](https://github.com/numaproj/numaflow-python/commit/c04a279b7930f79b1b9492ca81df24648b4dd511) fix: multiproc mapper max threads and default numprocess (#112)

### Contributors

 * Avik Basu

## v0.5.1 (2023-09-08)

 * [c5a4695](https://github.com/numaproj/numaflow-python/commit/c5a4695c0cdd571c683e4b6fcdac785300a16952) feat: add side input grpc service  (#107)

### Contributors

 * Sidhant Kohli

## v0.5.0 (2023-09-05)

 * [0be0b48](https://github.com/numaproj/numaflow-python/commit/0be0b486cec372f1577bde31d7ec031c45a321e8) Update flatmap stream sdk version dependency (#99)

### Contributors

 * xdevxy

## v0.4.2 (2023-06-07)

 * [15a6723](https://github.com/numaproj/numaflow-python/commit/15a6723a71070cf9bab583fe2ef0ddb5a800b097) feat!: support len, iter and indexing on udf datatypes (#96)
 * [c441ebe](https://github.com/numaproj/numaflow-python/commit/c441ebedfdd3880619cb6a2cef0889b3b92e6473) feat: introduce `MapStreamFn` for streaming map udf in async sever (#93)
 * [97d7f4e](https://github.com/numaproj/numaflow-python/commit/97d7f4efdcc0ae759df2a3d28b7c22253b4f33b8) fix: update event time filter transformer to use tag (#92)

### Contributors

 * Avik Basu
 * Keran Yang
 * xdevxy

## v0.4.1 (2023-04-24)

 * [ee753f5](https://github.com/numaproj/numaflow-python/commit/ee753f5074d135fd596b8957402f25f68f788bc6) feat: introduce handshake to client and gRPC server (#89)

### Contributors

 * Sidhant Kohli

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

