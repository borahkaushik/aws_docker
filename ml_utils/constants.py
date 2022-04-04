import  pyarrow as pa

class MLConstants:
    EXACT_MATCHES_SCHEMA = [
                pa.field('uuid_a',pa.string()),
                pa.field('uuid_b',pa.string()),
                pa.field('score', pa.float64())
            ]
    
    ID_UUID_SCHEMA =  [
            pa.field('id',pa.int64()),
            pa.field('uuid', pa.string())
        ]
    
    ID_INPUT_ID_SCHEMA =  [
            pa.field('id',pa.int64()),
            pa.field('input_ids', pa.list_(pa.int64()))
        ] 
    
    ID_EMBEDDINGS_SCHEMA = [
            pa.field('id',pa.int64()),
            pa.field('embeddings', pa.list_(pa.float64()))
        ]