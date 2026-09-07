# Introduction

Nestor is a read-through block cache for S3-compatible object storage. Objects are split into fixed-size blocks held in RAM and on local disk. A read of any byte range resolves to the blocks covering it and misses are fetched from the origin.

It runs as a Rust library embedded in your service or as the `nestor` binary, an S3 endpoint that serves GET and HEAD from cache and forwards everything else to the origin.

Documentation is being written. The [README](https://github.com/picomq/nestor#readme) has install and quick start instructions in the meantime.
