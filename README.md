# Python/Pandas read/write concurrency demos

This repo explores reading/writing CSVs using Pandas, testing different concurrency options:

  - "Traditional" single-threaded code.
  - Multi-threaded using [concurrent.futures.ThreadPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor).
  - Multi-process using [concurrent.futures.ProcessPoolExecutor](https://docs.python.org/3/library/concurrent.futures.html#processpoolexecutor).

I ran this study only on one computer; your results may differ.

## Conclusion

For CSV files of 1+ MB, you should always use multi-threading when reading more than one file.

## Summary

For reading CSVs, there is a clear speed advantage to using multi-threaded, except for the smallest files (~100 KB).  Even when loading only 2x 1 MB files, there is a noticable speed improvement.  The speed improvement is greater for larger files, and scales approximately linearly with file count.

There is no noticably advantage to scaling the thread count above the default (5 * core count).  There may be an advantage here for very large files, though you may run eventually into memory problems.

Multi-process is never as good as multi-threading, though does beat single-threaded for the largest file sizes (presumably as each process is effectively a different thread).  However, for smaller files the overhead of starting new processes is not worth it.

## Read CSV

Step to run:

  - Update [config.py](config.py) to change drives/paths to your own computer.
  - Execute the [make_files.py](make_files.py) script to run the test.

### Results

Results on my computer (i7-9700K 8-core, 32 GB DD4, Samsung SSD 840 EVO 250 GB *)

| File count | File size (MB) | Single threaded | Multi threaded (x40) | Multi-threaded (1 per file) | Multi-process (x8) |
| ---------- | -------------- | --------------- | -------------------- | --------------------------- | ------------------ |
| 2          | ~0.1           | 0.077177        | 0.071879             | 0.070994                    | 6.756075           |
|            | ~1             | 0.195387        | 0.140956             | 0.138384                    | 6.794708           |
|            | ~10            | 1.502026        | 0.985716             | 0.968148                    | 7.604471           |
|            | ~20            | 3.057094        | 1.804786             | 1.840519                    | 8.595857           |
| 10         | ~0.1           | 0.286254        | 0.327260             | 0.320471                    | 6.765667           |
|            | ~1             | 0.979133        | 0.497742             | 0.479304                    | 7.016924           |
|            | ~10            | 7.316718        | 2.152414             | 2.160644                    | 9.283074           |
|            | ~20            | 15.119492       | 3.758084             | 3.867373                    | 11.707926          |
| 100        | ~0.1           | 2.842518        | 3.107168             | 3.178195                    | 7.583967           |
|            | ~1             | 9.760879        | 4.823537             | 4.911401                    | 9.093502           |
|            | ~10            | 72.634632       | 19.870446            | 19.994871                   | 29.793505          |
|            | ~20            | 151.373390      | 36.290062            | 35.592058                   | 48.973783          |

\* *I also tested on an M.2. SSD and a HDD.  I did not notice much performance difference between them for this test.*
