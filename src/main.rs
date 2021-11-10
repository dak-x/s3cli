use std::path;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3 as s3;
use s3::model::{
    BucketLocationConstraint, CompletedMultipartUpload, CompletedPart, CreateBucketConfiguration,
};
use s3::output::{CreateMultipartUploadOutput, UploadPartOutput};
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

    ExistBucket {
        bucket: String,
    },

    ListBuckets,
    ListObjects {
        bucket: String,
    },

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

    MultipartUpload {
        bucket: String,
        key: String,
    },

    ListMultiparts {
        bucket: String,
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

        S3Operation::DeleteObject { bucket, key } => {
            let delete_resp = client.delete_object().bucket(bucket).key(key).send().await;
            println!("Resp: {:?}", delete_resp);
        }

        S3Operation::GetObject { bucket, key } => {
            let requested_object = client.get_object().bucket(bucket).key(key).send().await;

            match requested_object {
                Err(err) => println!("Error: {}", err),
                Ok(resp_obj) => resp_obj
                    .body
                    .collect()
                    .await
                    .map(|byte_stream| {
                        let st = String::from_utf8(byte_stream.into_bytes().to_vec());
                        println!("Object Recv: {:#?}", st);
                    })
                    .map_err(|err| println!("Streaming Error: {}", err))
                    .unwrap_or(()),
            }
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

        S3Operation::ListObjects { bucket } => {
            let all_objects = client.list_objects().bucket(bucket).send().await;

            match all_objects {
                Err(err) => {
                    println!("Err {}", err);
                }
                Ok(objects) => {
                    println!("Objects in Bucket: {}:", bucket);

                    match objects.contents {
                        None => println!("No Objects in the bucket"),
                        Some(objects) => {
                            for (idx, object) in objects.iter().enumerate() {
                                println!("{}: {:?}", idx, object);
                            }
                        }
                    }
                }
            }
        }

        S3Operation::MultipartUpload { bucket, key } => {
            handle_multipart(client, bucket, key).await;
        }

        S3Operation::ExistBucket { bucket } => {
            let resp = client.head_bucket().bucket(bucket).send().await;
            println!("{:?}", resp);
        }

        S3Operation::ListMultiparts { bucket } => {
            let resp = client.list_multipart_uploads().bucket(bucket).send().await;
            println!("{:?}", resp.unwrap().uploads);
        }
        _ => {
            println!("Not Yet Implemented");
        }
    }
}

async fn handle_multipart(client: Client, bucket: &str, key: &str) {
    // Takes file part names to upload the objects in sequence.

    // It's 3 Step process.
    // Step 1: Initiate a Multipart Thing.
    let initiation = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key.clone())
        .send()
        .await;

    if initiation.is_err() {
        println!("Err: {}", initiation.unwrap_err());
        return;
    }

    let CreateMultipartUploadOutput {
        bucket, upload_id, ..
    } = initiation.unwrap();

    println!("1. Initiated MultiPart Upload.");

    println!("2. Sequencially enter the file names: ");

    let stdin = std::io::stdin();
    let mut buf = String::new();
    let mut part_number = 0;
    let mut completed_parts = Vec::new();

    // Step2: Upload all the parts one by one.
    // Aborting commands isn't allowed right now.
    'outer: loop {
        if let Ok(n) = stdin.read_line(&mut buf) {
            // Nothing is read => ctrl+d is hit maybe..
            if n == 0 {
                buf = String::from("END");
            }

            if buf == "END" {
                client
                    .complete_multipart_upload()
                    .upload_id(upload_id.clone().unwrap())
                    .bucket(bucket.clone().unwrap())
                    .key(key)
                    .multipart_upload(
                        CompletedMultipartUpload::builder()
                            .set_parts(Some(completed_parts))
                            .build(),
                    )
                    .send()
                    .await
                    .unwrap();

                println!("Completed Mutlipart Upload.");
                break 'outer;
            }
            buf.pop();
            let body = ByteStream::from_path(buf.clone()).await.unwrap();
            part_number += 1;
            let resp = client
                .upload_part()
                .body(body)
                .bucket(bucket.clone().unwrap())
                .key(key.clone())
                .part_number(part_number)
                .upload_id(upload_id.clone().unwrap())
                .send()
                .await
                .unwrap();

            completed_parts.push(
                CompletedPart::builder()
                    .e_tag(resp.e_tag.unwrap())
                    .part_number(part_number)
                    .build(),
            );
        } else {
            // End the object updation.
            client
                .complete_multipart_upload()
                .upload_id(upload_id.clone().unwrap())
                .bucket(bucket.clone().unwrap())
                .key(key)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(completed_parts))
                        .build(),
                )
                .send()
                .await
                .unwrap();

            println!("Completed Mutlipart Upload.");

            break 'outer;
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
