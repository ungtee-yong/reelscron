import { makeR2Client } from "./r2.js";
import { pickAndLockRandomJob, markJobDone } from "./pickJob.js";
import { postReel } from "./postReel.js";

async function main() {
  const s3 = makeR2Client();

  const picked = await pickAndLockRandomJob(s3);
  if (!picked) {
    console.log("No ready jobs. Exit.");
    return;
  }

  const { job, processingKey } = picked;

  try {
    const result = await postReel(job);
    console.log("Post result:", result);
    await markJobDone(s3, processingKey, result);
    console.log("Marked done:", processingKey);
  } catch (err) {
    console.error("Post failed:", err);
    // ถ้าอยากทำ retry: ย้ายกลับไป ready หรือย้ายไป failed/
    // เพื่อความง่าย ตอนนี้ค้างไว้ใน processing ให้คุณดู log ก่อน
    process.exitCode = 1;
  }
}

main();
