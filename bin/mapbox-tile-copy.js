// Our goal is a command that can be invoked something like this:
// $ mapbox-tile-copy /path/to/some/file s3://bucket/folder/{z}/{x}/{y}.png
// $ mapbox-tile-copy /path/to/some/file s3://bucket/{prefix}/folder/{z}/{x}/{y}.vector.pbf
//
// From the perspective of using this as a command, some choices
// - should this assume you already ran mapbox-upload-validate on your file?
// - what should the outputs be?
