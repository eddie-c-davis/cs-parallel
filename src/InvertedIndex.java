/**
 * Eddie Davis
 * CS530 - PA3
 * Better Inverted Index
 * InvertedIndex.java
 * 11/9/2016
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Builds an inverted index: each word followed by files it was found in.
 */
public class InvertedIndex {
    /**
     * FileCountWritable Class: a custom Writable for maintaining file name and counts for each word.
     */
    public static class FileCountWritable implements WritableComparable {
        private String file = "";
        private Integer count = 0;

        public  FileCountWritable(Integer c, String f) {
            set(c, f);
        }

        public FileCountWritable() {
            this(0, "");
        }

        public FileCountWritable(FileCountWritable other) {
            this(other.getCount(), other.getFile());
        }

        public void set(Integer c, String f) {
            count = c;
            file = f;
        }

        public String getFile() {
            return file;
        }

        public Integer getCount() {
            return count;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(count);
            out.writeUTF(file);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            count = in.readInt();
            file = in.readUTF();
        }

        @Override
        public int compareTo(Object obj) {
            FileCountWritable other = (FileCountWritable) obj;

            int comp = count.compareTo(other.getCount());
            if (comp == 0) {
                comp = file.compareTo(other.getFile());
            }

            return comp;

        }

        @Override
        public boolean equals(Object obj) {
            return (this.compareTo(obj) == 0);
        }

        @Override
        public int hashCode() {
            return count.hashCode() + file.hashCode();
        }

        @Override
        public String toString() {
            return count + " " + file;
        }
    }

    /**
     * Mapper Class
     */
	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, FileCountWritable> {
		private final static String pattern = " , .;:'\"&!?-_\n\t12345678910[]{}<>\\`~|=^()@#$%^*/+-";
		private final static Text word = new Text();
        private final static FileCountWritable fileCount = new FileCountWritable();

        private static HashMap<String, HashMap<String, Integer>> fileMap;

        /**
         * Setup the in-mapper combiner hash map.
         *
         * @param context
         */
        public void setup(Context context) {
            fileMap = new HashMap<>();
        }

        /**
         * Map the input keys (file names and words), and keep a running count using the in-mapper combiner.
         *
         * @param key
         * @param val
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
		public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            String line = val.toString().trim();
            if (!line.isEmpty()) {      // Skip empty strings...
                // Get file name...
                FileSplit fileSplit = (FileSplit) context.getInputSplit();
                String fileName = fileSplit.getPath().getName();

                // Ensure a word map exists for this file...
                if (!fileMap.containsKey(fileName)) {
                    fileMap.put(fileName, new HashMap<String, Integer>());
                }

                HashMap<String, Integer> wordMap = fileMap.get(fileName);

                // Tokenize the string and update the word map...
                StringTokenizer iter = new StringTokenizer(line.toLowerCase(), pattern);
                while (iter.hasMoreTokens()) {
                    String token = iter.nextToken();
                    if (!wordMap.containsKey(token)) {
                        wordMap.put(token, 0);
                    }

                    wordMap.put(token, wordMap.get(token) + 1);
                }
            }
		}

        /**
         * Cleanup the mapper, invoke the emit method to write the in-mapper combiner to the given context.
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void cleanup(Context context) throws IOException, InterruptedException {
            emit(context);
        }

        /**
         * Emit the hash map using the in-mapper combiner pattern.
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        private void emit(Context context) throws IOException, InterruptedException {
            // Emit word maps for each file...
            for (String fileName : fileMap.keySet()) {
                HashMap<String, Integer> wordMap = fileMap.get(fileName);
                for (String token : wordMap.keySet()) {
                    word.set(token);
                    fileCount.set(wordMap.get(token), fileName);
                    context.write(word, fileCount);
                }

                wordMap.clear();    // Clear at each step in case this method is called more than once during mapping...
            }

            fileMap.clear();
        }
	}

    /**
     * Reducer Class
     */
	public static class InvertedIndexReducer extends Reducer<Text, FileCountWritable, Text, Text> {
        /**
         * Reduce the file counts into one set for each word, reverse sorting so that the files with most occurrences appear first.
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
		public void reduce(Text key, Iterable<FileCountWritable> values, Context context) throws IOException, InterruptedException {
            /**
             * Convert the iterator into a list...
             */
            Iterator<FileCountWritable> iter = values.iterator();
            List<FileCountWritable> list = new ArrayList<>();
            while (iter.hasNext()) {
                list.add(new FileCountWritable(iter.next()));
            }

            // Sort the list by word count (see FileCountWritable.compareTo method).
            if (list.size() > 1) {
                Collections.sort(list);
            }

			StringBuilder toReturn = new StringBuilder();

            // Walk the list backward to achieve reverse sorting.
            int end = list.size() - 1;
            for (int i = end; i >= 0; i--) {
				if (i < end) {
					toReturn.append(", ");
                }

                FileCountWritable fileCount = list.get(i);
                toReturn.append(fileCount.toString());
			}

			context.write(key, new Text(toReturn.toString()));
		}
	}

    /**
     * Main method: sets up map reduce job, times, and runs it.
     *
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		int status;
		if (args.length > 1) {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "InvertedIndex");
            job.setJarByClass(InvertedIndex.class);
            job.setMapperClass(InvertedIndexMapper.class);
            job.setReducerClass(InvertedIndexReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FileCountWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.out.println("Running with input=" + args[0] + ", output=" + args[1]);

            long t_i = System.nanoTime();
            status = job.waitForCompletion(true) ? 0 : 1;
            long t_r = (System.nanoTime() - t_i) / 1000000L;

            System.out.println(String.format("Runtime: %d ms", t_r));
        } else {
            System.out.println("Usage: InvertedIndex <input path> <output path>");
            status = 1;
        }

        System.exit(status);
	}
}
