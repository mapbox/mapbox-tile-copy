## work-in-progress

## Running tests

Tests involve copying files to S3. You can bring your own bucket by specifying a `TestBucket` environment variable.
```sh
$ TestBucket=my-bucket npm test
```

If you don't specify a bucket, it will attempt to write to a private Mapbox bucket, and will fail if your environment is not configured with appropriate credentials.
