import ApacheBeam

public extension PCollection {

    func capture<K,V>(captured: AsyncStream<(K,[V])>.Continuation,_ name: String? = nil, _file: String = #fileID, _line: Int = #line) -> PCollection<Of> where Of == KV<K,V> {
        map { input in
            print("\(input)")
            captured.yield((input.key, input.values))
            return input
        }
    }
    
    func asMap<K: Hashable, V>(_ name: String? = nil, _file: String = #fileID, _line: Int = #line) async throws -> [K:V] where Of == KV<K,V> {
        var output: [K:V] = [:]
        let (s,c) = AsyncStream.makeStream(of:(K,V).self)
        pstream { input in
            for try await (kv,_,_) in input {
                if let v = kv.value {
                    c.yield((kv.key,v))
                }
            }
            c.finish()
        }
        for await kv in s {
            output[kv.0] = kv.1
        }
        return output
    }
}
