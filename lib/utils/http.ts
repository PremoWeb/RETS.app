import * as https from "https";

export function makeRetsRequest(
  options: https.RequestOptions & { responseType?: "arraybuffer" | "text" },
  data?: string
): Promise<{ response: string | Buffer; headers: any }> {
  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      const chunks: Buffer[] = [];
      const contentLength = parseInt(res.headers["content-length"] || "0", 10);
      let receivedLength = 0;

      res.on("data", (chunk: Buffer) => {
        chunks.push(Buffer.from(chunk));
        receivedLength += chunk.length;
      });

      res.on("end", () => {
        const buffer = Buffer.concat(chunks);
        if (options.responseType === "arraybuffer") {
          resolve({ response: buffer, headers: res.headers });
        } else {
          const textResponse = buffer.toString("utf8");
          resolve({ response: textResponse, headers: res.headers });
        }
      });

      res.on("error", (error) => {
        reject(error);
      });
    });

    req.on("error", (err) => {
      reject(err);
    });

    if (data) req.write(data);
    req.end();
  });
}
