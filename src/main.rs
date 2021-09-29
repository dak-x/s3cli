use std::path;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3 as s3;
use s3::model::{BucketLocationConstraint, CreateBucketConfiguration};
use s3::{ByteStream, Client, Error, Region};

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
struct S3Command {
    #[structopt(short, long)]
    region: Option<String>,

    #[structopt(subcommand)]
    operation: S3Operation,
}

#[derive(StructOpt, Debug)]
enum S3Operation {
    CreateBucket {
        #[structopt(name = "Bucket Name")]
        bucket: String,
        #[structopt(long = "re")]
        region: Option<String>,
    },

    DeleteBucket {
        bucket: String,
    },

    ListBuckets,

    CreateObject {
        bucket: String,
        key: String,
        #[structopt(parse(from_os_str))]
        obj: path::PathBuf,
    },

    DeleteObject {
        bucket: String,
        key: String,
    },

    GetObject {
        bucket: String,
        key: String,
    },
}

async fn execute_operation(client: Client, oper: S3Command) {
    match &oper.operation {
        S3Operation::CreateBucket { bucket, region } => {
            let bucket_region = region
                .as_ref()
                .map(|region| BucketLocationConstraint::from(region.as_str()))
                .unwrap_or_else(|| match &oper.region {
                    Some(region) => BucketLocationConstraint::from(region.as_str()),
                    // Default region, can be set in the config.
                    None => BucketLocationConstraint::from("us-west-2"),
                });

            let bucket_config = CreateBucketConfiguration::builder()
                .location_constraint(bucket_region)
                .build();

            let resp = client
                .create_bucket()
                .bucket(bucket)
                .create_bucket_configuration(bucket_config)
                .send()
                .await;

            println!("Resp: {:#?}", resp);
        }
        S3Operation::CreateObject { bucket, key, obj } => {
            let obj_stream = ByteStream::from_path(obj).await.unwrap();

            let create_resp = client
                .put_object()
                .bucket(bucket)
                .body(obj_stream)
                .key(key)
                .send()
                .await;

            println!("Resp: {:?}", create_resp);
        }

        S3Operation::DeleteBucket { bucket: _ } => {
            println!("Not Yet Implemented");
        }

        S3Operation::DeleteObject { bucket, key } => {
            let delete_resp = client.delete_object().bucket(bucket).key(key).send().await;
            println!("Resp: {:?}", delete_resp);
        }

        S3Operation::GetObject { bucket, key } => {
            let requested_object = client.get_object().bucket(bucket).key(key).send().await;
            println!("Resp: {:?}", requested_object);
        }

        S3Operation::ListBuckets => {
            let all_buckets = client.list_buckets().send().await;

            match all_buckets {
                Err(err) => {
                    println!("Err: {}", err);
                    return;
                }

                Ok(buckets) => {
                    println!("List of Buckets: ");

                    match buckets.buckets {
                        None => println!("No buckets found."),
                        Some(buckets) => {
                            for (idx, bucket) in buckets.iter().enumerate() {
                                println!("{}: {:?}", idx, bucket);
                            }
                        }
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let S3Command { region, operation } = S3Command::from_args();

    // println!("Executing {:?}....", opt);

    let region_provider = RegionProviderChain::first_try(region.clone().map(Region::new))
        .or_default_provider()
        .or_else(Region::new("us-west-2"));

    let shared_config = aws_config::from_env().region(region_provider).load().await;
    let client = Client::new(&shared_config);

    execute_operation(client, S3Command { region, operation }).await;

    Ok(())
}
