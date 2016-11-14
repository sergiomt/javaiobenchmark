import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Random;

import java.util.concurrent.Semaphore;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import java.util.function.Consumer;

import java.util.stream.Stream;

/**
 * Benchmark read text files simulating log processing
 * @author Sergio Montoro Ten
 *
 */
public class FileReadBenchmark {

	private static DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final int Kb = 1024;
	private static final int Mb = 1024*Kb;
	private final String filenamePreffix = "LogFile";
	private final String LF = "\n";
	private final String DL = ";";
	private final char DL0 = ';';
	private final boolean profilingEnabled = true;
	private int overrunCount;
	private Semaphore consumerThreads;
	private LinkedHashMap<String,long[]> chronometers;

	public static void main(String args[]) throws Exception {
		final String targetDir = "D:\\Temp\\FileReadBenchmark\\";
		final int numberOfFiles = 204;
		final int fileSizeKb = 4*Mb;
		final int bufferSize = 32*Kb;
		final int degreeOfParalelism = 3;

		System.out.println("Start benchmark");
		System.out.println("");

		Runtime runtime = Runtime.getRuntime();

		FileReadBenchmark processor = new FileReadBenchmark(numberOfFiles, degreeOfParalelism);

		// Time to parse a non-cached file
		processor.generateFiles(targetDir, numberOfFiles, fileSizeKb);
		runtime.gc();
		processor.resetAllChronometers();
		processor.rawParse(targetDir, numberOfFiles);
		processor.printReport("Raw Parse 1st run", numberOfFiles*fileSizeKb);
		processor.generateFiles(targetDir, numberOfFiles, fileSizeKb);
		runtime.gc();
		processor.resetAllChronometers();
		processor.lineReaderParse(targetDir, numberOfFiles);
		processor.printReport("Line Reader 1st run", numberOfFiles*fileSizeKb);
		processor.generateFiles(targetDir, numberOfFiles, fileSizeKb);
		runtime.gc();
		processor.resetAllChronometers();
		processor.lineReaderParseParallel(targetDir, numberOfFiles, degreeOfParalelism);
		processor.printReport("Parallel Line Reader 1st run", numberOfFiles*fileSizeKb);
		processor.generateFiles(targetDir, numberOfFiles, fileSizeKb);
		runtime.gc();
		processor.resetAllChronometers();
		processor.nioFilesParse(targetDir, numberOfFiles);
		processor.printReport("NIO lines 1st run", numberOfFiles*fileSizeKb);
		processor.resetAllChronometers();
		processor.generateFiles(targetDir, numberOfFiles, fileSizeKb);
		runtime.gc();
		processor.nioAsyncParse(targetDir, numberOfFiles, degreeOfParalelism, bufferSize);
		processor.printReport("NIO Async lines 1st run", numberOfFiles*fileSizeKb);
		processor.generateFiles(targetDir, numberOfFiles, fileSizeKb);
		runtime.gc();
		processor.resetAllChronometers();
		processor.nioMemoryMappedParse(targetDir, numberOfFiles);
		processor.printReport("NIO Memory Mapped ByteBuffer lines 1st run", numberOfFiles*fileSizeKb);

		System.out.println("");

		System.out.println("End benchmark");
	}

	public FileReadBenchmark(int numberOfFiles, int degreeOfParalelism) {
		chronometers = new LinkedHashMap<String,long[]>();
		consumerThreads = new Semaphore(degreeOfParalelism);
	}

	/**
	 * Process log files using Java IO InputStream
	 */
	public void rawParse(final String targetDir, final int numberOfFiles) throws IOException, ParseException {
		overrunCount = 0;
		final int dl = DL.charAt(0);
		if (profilingEnabled) {
			startChronometer("end-to-end");
			startPausedChronometer("iterate lines");
			startPausedChronometer("open file");
		}
		StringBuffer lineBuffer = new StringBuffer(1024);
		for (int f=0; f<numberOfFiles; f++) {
			if (profilingEnabled)
				restartChronometer("open file");
			File fl = new File(targetDir+filenamePreffix+String.valueOf(f)+".txt");
			FileInputStream fin = new FileInputStream(fl);
			BufferedInputStream bin = new BufferedInputStream(fin);
			if (profilingEnabled) {
				pauseChronometer("open file");
				restartChronometer("iterate lines");
			}
			int character;
			while((character=bin.read())!=-1) {
				if (character==dl) {
					doSomethingWithRawLine(lineBuffer.toString());
					lineBuffer.setLength(0);					
				} else {
					lineBuffer.append((char) character);
				}
			}
			if (profilingEnabled)
				pauseChronometer("iterate lines");
			bin.close();
			fin.close();
		}
		if (profilingEnabled)
			pauseChronometer("end-to-end");
	}

	/**
	 * Process log files using classic Java IO FileReader
	 */
	public void lineReaderParse(final String targetDir, final int numberOfFiles) throws IOException, ParseException {
		String line;
		overrunCount = 0;
		if (profilingEnabled) {
			startChronometer("end-to-end");
			startPausedChronometer("iterate lines");
			startPausedChronometer("open file");
		}
		for (int f=0; f<numberOfFiles; f++) {
			if (profilingEnabled)
				restartChronometer("open file");
			File fl = new File(targetDir+filenamePreffix+String.valueOf(f)+".txt");
			FileReader frd = new FileReader(fl);
			BufferedReader brd = new BufferedReader(frd);
			if (profilingEnabled) {
				pauseChronometer("open file");
				restartChronometer("iterate lines");
			}
			while ((line=brd.readLine())!=null) {
				doSomethingWithLine(line);
			}
			if (profilingEnabled)
				pauseChronometer("iterate lines");
			brd.close();
			frd.close();
		}
		if (profilingEnabled)
			pauseChronometer("end-to-end");
	}

	/**
	 * Process log files using several threads and classic Java IO FileReader
	 */
	public void lineReaderParseParallel(final String targetDir, final int numberOfFiles, final int degreeOfParalelism)
			throws IOException, ParseException, InterruptedException {
		Thread[] pool = new Thread[degreeOfParalelism];
		overrunCount = 0;
		if (profilingEnabled) {
			startChronometer("end-to-end");
		}
		int batchSize = numberOfFiles / degreeOfParalelism;
		for (int b=0; b<degreeOfParalelism; b++) {
			pool[b] = new LineReaderParseThread(targetDir, b*batchSize, b*batchSize+b*batchSize);
			pool[b].start();
		}
		for (int b=0; b<degreeOfParalelism; b++) {
			pool[b].join();
		}		
		if (profilingEnabled)
			pauseChronometer("end-to-end");
	}

	/**
	 * Process logs files using classic Java NIO Stream
	 */
	public void nioFilesParse(final String targetDir, final int numberOfFiles) throws IOException, ParseException {
		overrunCount = 0;
		if (profilingEnabled) {
			startChronometer("end-to-end");
			startPausedChronometer("iterate lines");
			startPausedChronometer("open file");
		}
		for (int f=0; f<numberOfFiles; f++) {
			restartChronometer("open file");
			Path ph = Paths.get(targetDir+filenamePreffix+String.valueOf(f)+".txt");
			Consumer<String> action = new LineConsumer();
			if (profilingEnabled) {
				pauseChronometer("open file");
				restartChronometer("iterate lines");
			}
			Stream<String> lines = Files.lines(ph);
			lines.forEach(action);
			if (profilingEnabled)
				pauseChronometer("iterate lines");
			lines.close();
		}
		if (profilingEnabled)
			pauseChronometer("end-to-end");
	}

	/**
	 * Process log files using classic Java NIO AsynchronousFileChannel
	 */
	public void nioAsyncParse(final String targetDir, final int numberOfFiles, final int numberOfThreads, final int bufferSize) throws IOException, ParseException, InterruptedException {
		if (numberOfFiles%numberOfThreads!=0)
			throw new IllegalArgumentException("The number of files must be a multiple of the number of threads");
		if (profilingEnabled) {
			startChronometer("end-to-end");
		}
		ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(numberOfThreads);
		ConcurrentLinkedQueue<ByteBuffer> byteBuffers = new ConcurrentLinkedQueue<ByteBuffer>();
		for (int b=0; b<numberOfThreads; b++)
			byteBuffers.add(ByteBuffer.allocate(bufferSize));
		for (int f=0; f<numberOfFiles; f++) {
			consumerThreads.acquire();
			String fileName = targetDir+filenamePreffix+String.valueOf(f)+".txt";
			AsynchronousFileChannel channel = AsynchronousFileChannel.open(Paths.get(fileName), EnumSet.of(StandardOpenOption.READ), pool);
			BufferConsumer consumer = new BufferConsumer(byteBuffers, fileName, bufferSize);
			channel.read(consumer.buffer(), 0l, channel, consumer);
		}
		consumerThreads.acquire(numberOfThreads);
		if (profilingEnabled)
			pauseChronometer("end-to-end");
		consumerThreads.release(numberOfFiles);
	}

	/**
	 * Process logs files using memory mapped files
	 */
	public void nioMemoryMappedParse(final String targetDir, final int numberOfFiles) throws IOException, ParseException, InterruptedException {
		if (profilingEnabled) {
			startChronometer("end-to-end");
		}
		StringBuffer chars = new StringBuffer(65536);
		for (int f=0; f<numberOfFiles; f++) {
			consumerThreads.acquire();
			String fileName = targetDir+filenamePreffix+String.valueOf(f)+".txt";
			FileChannel channel = FileChannel.open(Paths.get(fileName), EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE));
			MappedByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0l, channel.size());
			final int len = byteBuffer.limit();
			int i = 0;
			try {
				for (i = 0; i < len; i++) {
					byte by = byteBuffer.get();
					if (by=='\n') {
						// ***
						// The code used to process the line goes here
						String[] fields = chars.toString().split(DL);
						if (fields.length>1) {
							Date dt = fmt.parse(fields[0]);
							double d = Double.parseDouble(fields[1]);
							int t = Integer.parseInt(fields[2]);
							if (fields[3].equals("overrun"))
								overrunCount++;
						}
						// End code used to process the line
						// ***
						chars.setLength(0);
					} else {
						chars.append((char) by);
					}
				}
			} catch (Exception x) {
				System.out.println("Caugth exception "+x.getClass().getName()+" "+x.getMessage()+" i="+String.valueOf(i)+", limit="+String.valueOf(len));
			}			
		}
		if (profilingEnabled)
			pauseChronometer("end-to-end");
		consumerThreads.release(numberOfFiles);
	}

	/**
	 * Split each line calling String.split() then do something with read values
	 */
	@SuppressWarnings("unused")
	public final void doSomethingWithLine(String line) throws ParseException {
		// What to do for each line
		String[] fields = line.split(DL);
		Date dt = fmt.parse(fields[0]);
		double d = Double.parseDouble(fields[1]);
		int t = Integer.parseInt(fields[2]);
		if (fields[3].equals("overrun"))
			overrunCount++;
	}

	/**
	 * Extract field values from a line on the fly by looking for column delimiters
	 */
	@SuppressWarnings("unused")
	public final void doSomethingWithRawLine(String line) throws ParseException {
		// What to do for each line
		int fieldNumber = 0;
		final int len = line.length();
		StringBuffer fieldBuffer = new StringBuffer(256);
		for (int charPos=0; charPos<len; charPos++) {
			char c = line.charAt(charPos);
			if (c==DL0) {
				String fieldValue = fieldBuffer.toString();
				if (fieldValue.length()>0) {
					switch (fieldNumber) {
					case 0:
						Date dt = fmt.parse(fieldValue);
						fieldNumber++;
						break;
					case 1:
						double d = Double.parseDouble(fieldValue);
						fieldNumber++;
						break;
					case 2:
						int t = Integer.parseInt(fieldValue);
						fieldNumber++;
						break;
					case 3:
						if (fieldValue.equals("overrun"))
							overrunCount++;
						break;
					}
				}
				fieldBuffer.setLength(0);
			} else {
				fieldBuffer.append(c);
			}
		}
	}

	class LineReaderParseThread extends Thread {

		private String targetDir;
		private int fileFrom;
		private int fileTo;
		private DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		private int overrunCounter = 0;

		public LineReaderParseThread(String targetDir, int fileFrom, int fileTo) {
			this.targetDir = targetDir;
			this.fileFrom = fileFrom;
			this.fileTo = fileTo;
		}

		private void doSomethingWithTheLine(String line) throws ParseException {
			String[] fields = line.split(DL);
			Date dt = fmt.parse(fields[0]);
			double d = Double.parseDouble(fields[1]);
			int t = Integer.parseInt(fields[2]);
			if (fields[3].equals("overrun"))
				overrunCounter++;			
		}

		@Override
		public void run() {
			String line;
			for (int f=fileFrom; f<fileTo; f++) {
				File fl = new File(targetDir+filenamePreffix+String.valueOf(f)+".txt");
				try {
					FileReader frd = new FileReader(fl);
					BufferedReader brd = new BufferedReader(frd);
					while ((line=brd.readLine())!=null) {
						doSomethingWithTheLine(line);
					}
					brd.close();
					frd.close();
				} catch (IOException | ParseException ioe) { }
			}			
		}
	}

	/**
	 * Consumer for Java  NIO Stream.forEach()
	 * Split each line calling String.split() then do something with read values
	 */
	class LineConsumer implements Consumer<String> {
		@Override
		public void accept(String line) {
			// What to do for each line
			String[] fields = line.split(DL);
			if (fields.length>1) {
				try {
					Date dt = fmt.parse(fields[0]);
				} catch (ParseException e) { }
				double d = Double.parseDouble(fields[1]);
				int t = Integer.parseInt(fields[2]);
				if (fields[3].equals("overrun"))
					overrunCount++;
			}
		}
	}

	class BufferConsumer implements CompletionHandler<Integer, AsynchronousFileChannel> {

		private ConcurrentLinkedQueue<ByteBuffer> buffers;
		private ByteBuffer bytes;
		private String file;
		private StringBuffer chars;
		private int limit;
		private long position;
		private DateFormat frmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		public BufferConsumer(ConcurrentLinkedQueue<ByteBuffer> byteBuffers, String fileName, int bufferSize) {
			buffers = byteBuffers;
			bytes = buffers.poll();
			if (bytes==null)
				bytes = ByteBuffer.allocate(bufferSize);
			file = fileName;
			chars = new StringBuffer(bufferSize);
			frmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			limit = bufferSize;
			position = 0l;
		}

		public ByteBuffer buffer() {
			return bytes;    		  
		}

		@Override
		public synchronized void completed(Integer result, AsynchronousFileChannel channel) {
			if (result!=-1) {
				bytes.flip();
				final int len = bytes.limit();
				int i = 0;
				try {
					for (i = 0; i < len; i++) {
						byte by = bytes.get();
						if (by=='\n') {
							// ***
							// The code used to process the line goes here
							String[] fields = chars.toString().split(DL);
							if (fields.length>1) {
								Date dt = frmt.parse(fields[0]);
								double d = Double.parseDouble(fields[1]);
								int t = Integer.parseInt(fields[2]);
								if (fields[3].equals("overrun"))
									overrunCount++;
							}
							// End code used to process the line
							// ***
							chars.setLength(0);
						} else {
							chars.append((char) by);
						}
					}
				} catch (Exception x) {
					System.out.println("Caugth exception "+x.getClass().getName()+" "+x.getMessage()+" i="+String.valueOf(i)+", limit="+String.valueOf(len)+", position="+String.valueOf(position));
				}
				if (len==limit) {
					bytes.clear();
					position += len;
					channel.read(bytes, position, channel, this);
				} else {
					try { channel.close(); } catch (IOException e) { }
					consumerThreads.release();
					bytes.clear();
					buffers.add(bytes);
				}
			} else {
				try { channel.close(); } catch (IOException e) { }
				consumerThreads.release();
				bytes.clear();
				buffers.add(bytes);
			}
		}

		@Override
		public void failed(Throwable e, AsynchronousFileChannel channel) { }
	};

	/**
	 * Generate test files. Five columns per line with different data types.
	 */
	public void generateFiles(final String targetDir, final int numberOfFiles, final int fileSizeKb) throws IOException {
		StringBuffer line = new StringBuffer(1024);
		Random rnd = new Random();
		final String dateValue = "2016-10-11 14:00:22";
		final String stringValue = "Name of a resource";
		final String ipValue = "127.0.0.1";
		final String messageValue = "Message produced by resource";
		final String longerValue = "Some description about what happened to the resource while executing its stuff...";
		final int timeoutValue = 30000;
		final int overrunValue = 20000;
		// The size of each generated file will be fileSizeKb plus or minus the percentage specified here
		final int sizeVariability = 20;
		for (int f=0; f<numberOfFiles; f++) {
			File fl = new File(targetDir+filenamePreffix+String.valueOf(f)+".txt");
			if (fl.exists())
				fl.delete();
			// System.out.println("Generating file "+String.valueOf(f+1)+" of "+String.valueOf(numberOfFiles));
			FileOutputStream fs = new FileOutputStream(fl);
			final int sizeChangePct = rnd.nextInt(sizeVariability);
			final int maxFileSize = fileSizeKb + ((sizeChangePct % 2 == 0 ? 1 : -1) * fileSizeKb*sizeChangePct/100);
			for (int bytesWritten = 0; bytesWritten<maxFileSize; bytesWritten += line.length()) {
				int timeTaken = rnd.nextInt(timeoutValue);
				line.append(dateValue).append(DL).append(rnd.nextDouble()).append(DL).append(timeTaken).append(DL).append(timeTaken>overrunValue ? "overrun" : "ontime").append(DL).append(stringValue).append(DL).append(ipValue).append(DL).append(timeTaken%2==0 ? messageValue : longerValue).append(LF);
				bytesWritten += line.length();
				fs.write(line.toString().getBytes());
				line.setLength(0);
			}
			fs.close();
		}
	}

	public void startPausedChronometer(String counterName) {
		if (chronometers.containsKey(counterName))
			chronometers.remove(counterName);
		chronometers.put(counterName, new long[]{-1l,0l});
	}

	public void startChronometer(String counterName) {
		if (chronometers.containsKey(counterName))
			chronometers.remove(counterName);
		chronometers.put(counterName, new long[]{System.nanoTime(),0l});
	}

	public void pauseChronometer(String counterName) {
		long[] previous = chronometers.get(counterName);
		if (previous[1]==-1l)
			throw new IllegalStateException("Chronometer "+counterName+" is already paused");
		chronometers.replace(counterName, new long[]{-1l, previous[1]+(System.nanoTime()-previous[0])});
	}

	public void restartChronometer(String counterName) {
		long[] previous = chronometers.get(counterName);
		chronometers.replace(counterName, new long[]{System.nanoTime(), previous[1]});
	}

	public void updateChronometer(String counterName) {
		long[] previous = chronometers.get(counterName);
		if (previous[1]==-1l)
			throw new IllegalStateException("Chronometer "+counterName+" is paused");
		chronometers.replace(counterName, new long[]{System.nanoTime(), previous[1]+(System.nanoTime()-previous[0])});
	}

	public long getChronometerRunningTimeNanos(String counterName) {
		return chronometers.get(counterName)[1];
	}

	public long getChronometerRunningTimeMilis(String counterName) {
		return chronometers.get(counterName)[1]/1000000;
	}

	public void resetAllChronometers() {
		chronometers.clear();
	}

	public void printReport(String title, int kilobytesProcesses) {
		Runtime runtime = Runtime.getRuntime();
		System.out.print(title);
		System.out.print(" ");
		if (chronometers.containsKey("end-to-end"))
			System.out.print("end-to-end "+String.valueOf(getChronometerRunningTimeMilis("end-to-end"))+"ms");
		System.out.print(" ");
		System.out.print("used memory "+String.valueOf((runtime.totalMemory() - runtime.freeMemory())/Kb)+" Kb");
		System.out.println("");
	}
}
