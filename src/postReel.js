export async function postReel(job) {
    // TODO: ใส่ Meta Graph API จริง
    // ตอนนี้แค่จำลองว่าโพสสำเร็จ
    console.log("Posting Reel with:", job.asset?.video_url);
    return { ok: true, platform: job.targets?.[0]?.platform || "facebook" };
}
  