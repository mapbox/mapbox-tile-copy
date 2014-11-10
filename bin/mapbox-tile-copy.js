// Our goal is a command that can be invoked something like this:
// $ mapbox-tile-copy /path/to/some/file s3://bucket/folder/{z}/{x}/{y}.png
// $ mapbox-tile-copy /path/to/some/file s3://bucket/{prefix}/folder/{z}/{x}/{y}.vector.pbf
// - should url templates include file extensions? Does tilelive-s3 already handle that?
//
// From the perspective of using this as a command, some choices
// - should this assume you already ran mapbox-upload-validate on your file?
// - what should the outputs be?
// - how should we pass in job/part information?
