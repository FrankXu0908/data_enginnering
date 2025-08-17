import polars as pl
from minio import Minio
from io import BytesIO
import pyarrow.dataset as ds

# 配置S3文件系统
minio_client = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)

# 创建PyArrow文件系统接口
fs = pyarrow.fs.S3FileSystem(
    endpoint_override="localhost:9000",
    scheme="http",
    access_key="minioadmin",
    secret_key="minioadmin"
)

# 直接查询数据湖
dataset = ds.dataset(
    'qa-metadata', 
    filesystem=fs,
    format="parquet",
    partitioning=ds.partitioning(
        schema=pyarrow.schema([
            pyarrow.field("capture_hour", pyarrow.int8()),
            pyarrow.field("equipment_id", pyarrow.string())
        ])
    )
)

# 使用Polars查询
df = (
    pl.scan_pyarrow_dataset(dataset)
    .filter(
        (pl.col("capture_time") >= "2023-08-08 08:00") &
        (pl.col("focal_length") > 4.0)
    )
    .groupby("equipment_id")
    .agg(pl.count().alias("image_count"))
    .sort(pl.col("image_count"), descending=True)
    .collect()
)

# 数据湖 + AI模型集成（模拟缺陷标注）
for row in df.iter_rows(named=True):
    # 从MinIO取对应图像
    image_obj = minio_client.get_object("qa-images", row["image_uri"].split("/")[-1])
    image_data = BytesIO(image_obj.read())
    
    # 调用质检模型（伪代码）
    is_defect = defect_detection_model.predict(image_data)
    
    # 更新元数据
    update = pl.DataFrame({
        "image_uri": [row["image_uri"]],
        "is_defect": [1 if is_defect else 0]
    })
    
    # 增量写入Delta Lake格式（需安装deltalake库）
    DeltaTable("minio/qa-metadata/delta").update().where("image_uri = '{row['image_uri']}'").set({"is_defect": is_defect}).execute()