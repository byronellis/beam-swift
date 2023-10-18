# Apache Beam Swift SDK

Provides the [Apache Beam][0] SDK for Swift.

## Usage

Add the package dependency in your `Package.swift`:

```swift
.package(
    url: "https://github.com/apache/beam-swift",
    .branch("main")
)
```

Next, in your target add `ApacheBeam` to your dependencies:

```swift
.target(name: "MyPipeline",dependencies:[
    .product(name:"ApacheBeam",package:"beam-swift"),
],
```

## Documentation

[0]: https://beam.apache.org

                                                    
                                                    
                                                    
                                                    
                                                    
