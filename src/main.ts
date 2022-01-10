import path from 'path';
import fs from 'fs';
import { Readable, Transform, TransformCallback } from 'stream';

interface Log {
  id: string;
  email: string;
  message: string;
}

interface Tally {
  email: string;
  total: number;
}

interface FileTally {
  id: string;
  tally: Tally[];
}

class FileTransform extends Transform {
  globalTally: FileTally[] = [];

	_transform(chunk: Buffer, _encoding: BufferEncoding, callback: TransformCallback): void {
    // Get file path from stream
		const filePath = chunk.toString();
    // Read file from path
    const fileContent = fs.readFileSync(filePath, 'utf-8');
    // Parse log file content into JSON object and get id & logs
    const { id, logs } = JSON.parse(fileContent);
    const tally: Record<string, number> = {};
    const fileTally: FileTally = { id, tally: [] };

    // Count logs per email
    logs.forEach((log: Log) => {
      tally[log.email] = (tally[log.email] ?? 0) + 1;
    });
    
    // Format result as required
    fileTally.tally = Object.keys(tally).map((email: string): Tally => ({
      email,
      total: tally[email]
    }));
    
    // Push individual file tally to global tally
    this.globalTally.push(fileTally);
    
		callback(null);
	}

	_final(callback: (error?: Error | null) => void): void {
    // Finally flush the result
    this.push(JSON.stringify(this.globalTally, null, 2));
		callback();
	}
}

const run = async () => {
  const filePathStream = new Readable();
  const fileTransform = new FileTransform();
  // Create output file stream
  const outputStream = fs.createWriteStream('GlobalTally.json');
  filePathStream.pipe(fileTransform).pipe(outputStream);

  // Get log directory path
  const logDirPath = path.join(__dirname, '..', 'logs');
  // Get file name list in log directory
  const fileNameList = fs.readdirSync(logDirPath);
  // Create a readable stream from file name list
  fileNameList.forEach((fileName) => {
    // Get file path (log directory path + file name)
    const filePath = path.join(logDirPath, fileName);
    filePathStream.push(filePath);
  });
  filePathStream.push(null);
};

run();
