import {
    ListObjectsV2Command,
    GetObjectCommand,
    CopyObjectCommand,
    DeleteObjectCommand,
  } from "@aws-sdk/client-s3";
  
  const toText = async (stream) => {
    const chunks = [];
    for await (const chunk of stream) chunks.push(chunk);
    return Buffer.concat(chunks).toString("utf8");
  };
  
  function pickRandom(arr) {
    return arr[Math.floor(Math.random() * arr.length)];
  }
  
  export async function pickAndLockRandomJob(s3) {
    const bucket = process.env.R2_BUCKET;
    const readyPrefix = process.env.R2_READY_PREFIX || "jobs/ready/";
    const processingPrefix = process.env.R2_PROCESSING_PREFIX || "jobs/processing/";
  
    // 1) list ready jobs
    const listed = await s3.send(new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: readyPrefix,
    }));
  
    const objs = (listed.Contents || []).filter(o => o.Key && o.Key.endsWith(".json"));
    if (objs.length === 0) return null;
  
    // 2) pick random
    const chosen = pickRandom(objs);
    const readyKey = chosen.Key;
    const fileName = readyKey.split("/").pop();
    const processingKey = `${processingPrefix}${fileName}`;
  
    // 3) "lock" by move: copy -> delete
    await s3.send(new CopyObjectCommand({
      Bucket: bucket,
      CopySource: `${bucket}/${readyKey}`,
      Key: processingKey,
    }));
    await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: readyKey }));
  
    // 4) read job json
    const got = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: processingKey }));
    const jsonText = await toText(got.Body);
    const job = JSON.parse(jsonText);
  
    return { job, processingKey };
  }
  
  export async function markJobDone(s3, processingKey, result) {
    const bucket = process.env.R2_BUCKET;
    const donePrefix = process.env.R2_DONE_PREFIX || "jobs/done/";
    const fileName = processingKey.split("/").pop();
    const doneKey = `${donePrefix}${fileName}`;
  
    // แนบผลลัพธ์ลง history ก่อนย้าย (optional)
    // ถ้าคุณอยาก "แก้ json" ก่อนย้าย ให้ทำ PutObject เพิ่ม
    // ที่นี่จะย้ายไฟล์ไป done เฉยๆ เพื่อเรียบง่าย
    await s3.send(new CopyObjectCommand({
      Bucket: bucket,
      CopySource: `${bucket}/${processingKey}`,
      Key: doneKey,
    }));
    await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: processingKey }));
  }
  