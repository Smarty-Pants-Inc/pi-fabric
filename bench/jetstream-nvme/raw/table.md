| case | backend | c | ops/s | p50 ms | p99 ms | p99.9 ms | max ms | writer ops min/max |
|---|---|--:|--:|--:|--:|--:|--:|--:|
| single put | nats-always | 1 | 759 | 1.248 | 1.923 | 4.418 |  |  |
| single put | sqlite1c-FULL | 1 | 853 | 1.108 | 2.114 | 4.612 | 12.7 |  |
| single put | sqlite-FULL | 1 | 856 | 1.105 | 2.294 | 4.313 | 32.0 | 25691/25691 |
| single put | nats-default | 1 | 24,057 | 0.042 | 0.103 | 0.238 |  |  |
| single put | sqlite1c-NORMAL | 1 | 68,724 | 0.006 | 0.018 | 4.077 | 63.4 |  |
| single put | sqlite-NORMAL | 1 | 88,987 | 0.004 | 0.012 | 3.909 | 214.3 | 2670645/2670645 |
| single put | nats-always | 8 | 829 | 9.489 | 17.346 | 20.921 |  |  |
| single put | sqlite1c-FULL | 8 | 809 | 9.607 | 14.534 | 21.608 | 47.5 |  |
| single put | sqlite-FULL | 8 | 854 | 1.112 | 1.915 | 8.992 | 27251.8 | 0/14824 |
| single put | nats-default | 8 | 91,887 | 0.083 | 0.180 | 1.159 |  |  |
| single put | sqlite1c-NORMAL | 8 | 95,197 | 0.036 | 0.159 | 5.658 | 51.1 |  |
| single put | sqlite-NORMAL | 8 | 199,818 | 0.004 | 0.011 | 2.420 | 11840.3 | 413492/1010870 |
| single put | nats-always | 32 | 791 | 38.223 | 90.279 | 109.516 |  |  |
| single put | sqlite1c-FULL | 32 | 809 | 38.702 | 50.410 | 94.046 | 95.0 |  |
| single put | sqlite-FULL | 32 | 832 | 1.183 | 3.446 | 13741.635 | 29758.0 | 0/4101 |
| single put | nats-default | 32 | 124,157 | 0.226 | 1.040 | 3.523 |  |  |
| single put | sqlite1c-NORMAL | 32 | 93,301 | 0.145 | 5.627 | 6.944 | 21.7 |  |
| single put | sqlite-NORMAL | 32 | 189,314 | 0.004 | 0.015 | 2.923 | 10437.5 | 73500/322224 |
| 3-key batch | nats-always | 1 | 271 | 3.522 | 6.233 | 14.003 |  |  |
| 3-key batch | sqlite1c-FULL | 1 | 801 | 1.198 | 2.322 | 6.396 | 24.4 |  |
| 3-key batch | sqlite-FULL | 1 | 846 | 1.124 | 2.217 | 4.741 | 19.5 | 25384/25384 |
| 3-key batch | nats-default | 1 | 16,296 | 0.060 | 0.195 | 0.473 |  |  |
| 3-key batch | sqlite1c-NORMAL | 1 | 35,916 | 0.010 | 0.028 | 4.400 | 34.4 |  |
| 3-key batch | sqlite-NORMAL | 1 | 37,186 | 0.009 | 0.025 | 4.370 | 31.4 | 1119256/1119256 |
| 3-key batch | nats-always | 8 | 257 | 28.459 | 82.671 | 103.054 |  |  |
| 3-key batch | sqlite1c-FULL | 8 | 820 | 9.403 | 15.604 | 21.535 | 41.5 |  |
| 3-key batch | sqlite-FULL | 8 | 786 | 1.200 | 3.487 | 2535.350 | 26769.3 | 874/7240 |
| 3-key batch | nats-default | 8 | 45,202 | 0.162 | 0.645 | 2.923 |  |  |
| 3-key batch | sqlite1c-NORMAL | 8 | 35,809 | 0.076 | 5.461 | 6.824 | 90.7 |  |
| 3-key batch | sqlite-NORMAL | 8 | 74,220 | 0.010 | 0.043 | 5.267 | 7134.7 | 191880/394329 |
| 3-key batch | nats-always | 32 | 280 | 113.321 | 166.649 | 223.049 |  |  |
| 3-key batch | sqlite1c-FULL | 32 | 766 | 40.929 | 53.609 | 57.890 | 58.6 |  |
| 3-key batch | sqlite-FULL | 32 | 808 | 1.195 | 4.783 | 12240.810 | 28178.6 | 7/3039 |
| 3-key batch | nats-default | 32 | 51,949 | 0.548 | 2.046 | 4.468 |  |  |
| 3-key batch | sqlite1c-NORMAL | 32 | 36,615 | 0.304 | 6.259 | 11.164 | 36.2 |  |
| 3-key batch | sqlite-NORMAL | 32 | 79,500 | 0.010 | 0.037 | 6.972 | 13840.1 | 26005/115573 |
| 3-key batch, NATS atomic | nats-always | 1 | 236 | 3.995 | 6.701 | 15.279 |  |  |
| 3-key batch, NATS atomic | nats-default | 1 | 2,218 | 0.396 | 1.699 | 2.379 |  |  |
| 3-key batch, NATS atomic | nats-always | 8 | 250 | 31.413 | 55.331 | 67.558 |  |  |
| 3-key batch, NATS atomic | nats-default | 8 | 3,515 | 2.149 | 8.873 | 13.209 |  |  |
| 3-key batch, NATS atomic | nats-always | 32 | 243 | 130.450 | 229.428 | 266.097 |  |  |
| 3-key batch, NATS atomic | nats-default | 32 | 4,632 | 6.181 | 17.312 | 26.179 |  |  |
| stream publish 1 KiB | nats-always | 1 | 792 | 1.204 | 2.224 | 5.078 |  |  |
| stream publish 1 KiB | nats-default | 1 | 22,851 | 0.039 | 0.163 | 1.159 |  |  |
| stream publish 1 KiB | nats-always | 8 | 815 | 9.658 | 17.824 | 23.664 |  |  |
| stream publish 1 KiB | nats-default | 8 | 119,215 | 0.063 | 0.153 | 0.290 |  |  |
| stream publish 1 KiB | nats-always | 32 | 813 | 39.060 | 68.221 | 88.146 |  |  |
| stream publish 1 KiB | nats-default | 32 | 210,317 | 0.141 | 0.418 | 1.269 |  |  |
