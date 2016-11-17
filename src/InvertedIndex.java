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
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
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
    public static class FileCountWritable implements WritableComparable<FileCountWritable> {
        private IntWritable count;
        private Text file;

        public  FileCountWritable(IntWritable c, Text f) {
            set(c, f);
        }

        public  FileCountWritable(Integer c, String f) {
            set(c, f);
        }

        public FileCountWritable() {
            this(0, "");
        }

        public FileCountWritable(FileCountWritable other) {
            count = new IntWritable(other.getCount().get());
            file = new Text(other.getFile().toString());
        }

        public void set(Integer c, String f) {
            set(new IntWritable(c), new Text(f));
        }

        public void set(IntWritable c, Text f) {
            count = c;
            file = f;
        }

        public Text getFile() {
            return file;
        }

        public IntWritable getCount() {
            return count;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            count.write(out);
            file.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            count.readFields(in);
            file.readFields(in);
        }

        @Override
        public int compareTo(FileCountWritable other) {
            int comp = count.compareTo(other.getCount());
            if (comp == 0) {
                comp = file.compareTo(other.getFile());
            }

            return comp;
        }

        public boolean equals(FileCountWritable other) {
            return (this.compareTo(other) == 0);
        }

        @Override
        public boolean equals(Object obj) {
            return (this.equals((FileCountWritable) obj));
        }

        @Override
        public int hashCode() {
            return count.hashCode() * 163 + file.hashCode();
        }

        @Override
        public String toString() {
            return count + " " + file;
        }

        public byte[] serialize() throws IOException {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);

            // Serialize the data
            this.write(dos);
            byte[] bytes = bos.toByteArray();
            dos.close();

            return bytes;
        }
    }

    public static class FileCountComparator extends WritableComparator {
        protected FileCountComparator() {
            super(FileCountWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            // Since we are converter all strings to lower case, we can just compare the byte arrays directly!
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

//    public static class InvertedIndexWritable implements WritableComparable<InvertedIndexWritable> {
//        private Text word;
//        private IntWritable count;
//        private Text file;
//
//        public  InvertedIndexWritable(Text w, IntWritable c, Text f) {
//            set(w, c, f);
//        }
//
//        public  InvertedIndexWritable(String w, Integer c, String f) {
//            set(w, c, f);
//        }
//
//        public InvertedIndexWritable() {
//            this("", 0, "");
//        }
//
//        public InvertedIndexWritable(InvertedIndexWritable other) {
//            word = new Text(other.getWord().toString());
//            count = new IntWritable(other.getCount().get());
//            file = new Text(other.getFile().toString());
//        }
//
//        public void set(String w, Integer c, String f) {
//            set(new Text(w), new IntWritable(c), new Text(f));
//        }
//
//        public void set(Text w, IntWritable c, Text f) {
//            word = w;
//            count = c;
//            file = f;
//        }
//
//        public Text getWord() {
//            return word;
//        }
//
//        public IntWritable getCount() {
//            return count;
//        }
//
//        public Text getFile() {
//            return file;
//        }
//
//        @Override
//        public void write(DataOutput out) throws IOException {
//            word.write(out);
//            count.write(out);
//            file.write(out);
//        }
//
//        @Override
//        public void readFields(DataInput in) throws IOException {
//            word.readFields(in);
//            count.readFields(in);
//            file.readFields(in);
//        }
//
//        @Override
//        public int compareTo(InvertedIndexWritable other) {
//            int comp = word.compareTo(other.getWord());
//
//            if (comp == 0) {
//                comp = count.compareTo(other.getCount()) * -1;      // Invert the count comparison to get max 1st
//            }
//
//            if (comp == 0) {
//                comp = file.compareTo(other.getFile());
//            }
//
//            return comp;
//
//        }
//
//        public boolean equals(InvertedIndexWritable other) {
//            return (this.compareTo(other) == 0);
//        }
//
//        @Override
//        public boolean equals(Object obj) {
//            return (this.equals((InvertedIndexWritable) obj));
//        }
//
//        @Override
//        public int hashCode() {
//            return word.hashCode() + count.hashCode() + file.hashCode();
//        }
//
//        @Override
//        public String toString() {
//            return word + " " + count + " " + file;
//        }
//    }

//    public static class KeyComparator extends WritableComparator {
//        protected KeyComparator() {
//            super(InvertedIndexWritable.class, true);
//        }
//
//        @Override
//        public int compare(WritableComparable w1, WritableComparable w2) {
//            InvertedIndexWritable ndx1 = (InvertedIndexWritable) w1;
//            InvertedIndexWritable ndx2 = (InvertedIndexWritable) w2;
//
//            int comp = InvertedIndexWritable.compare(ndx1.getFirst(), ndx2.getFirst());
//            if (comp == 0) {
//                comp = -InvertedIndexWritable.compare(ndx1.getSecond(), ndx2.getSecond()); //reverse
//            }
//
//            return comp;
//        }
//    }

    /**
     * Mapper Class
     */
	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, FileCountWritable> {
    //public static class InvertedIndexMapper extends Mapper<LongWritable, Text, InvertedIndexWritable, NullWritable> {
		private final static String pattern = " , .;:'\"&!?-_\n\t12345678910[]{}<>\\`~|=^()@#$%^*/+-";
		private final static Text word = new Text();
        private final static FileCountWritable fileCount = new FileCountWritable();
        //private final static InvertedIndexWritable invIndex = new InvertedIndexWritable();

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
                    //invIndex.set(token, wordMap.get(token), fileName);
                    //context.write(invIndex, NullWritable.get());
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
    //public static class InvertedIndexReducer extends Reducer<InvertedIndexWritable, NullWritable, Text, Text> {
        private static final Text EMPTY = new Text("");

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
        //public void reduce(InvertedIndexWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
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

	private static int serializeTest() {
        FileCountWritable fcw1 = new FileCountWritable(25984, "Encyclopaedia.txt"); //list.get(0);
        FileCountWritable fcw2 = new FileCountWritable(25984, "Encyclopaedia.txt"); //list.get(0);
        //FileCountWritable fcw2 = new FileCountWritable(32, "Bill-of-Rights.txt"); //list.get(1);

        int comp = 0;
        try {
            byte[] b1 = fcw1.serialize();
            byte[] b2 = fcw2.serialize();
            int s1 = 0;
            int l1 = 0;
            int s2 = 0;
            int l2 = 0;

            // Counts
            int count1 = WritableComparator.readInt(b1, s1);
            int count2 = WritableComparator.readInt(b2, s2);

            comp = 0; //(count1 < count2) ? -1 : (count1 == count2) ? 0 : 1;
            if (comp == 0) {
                s1 += 4;
                s2 += 4;

                // String sizes
                int i1 = WritableComparator.readVInt(b1, s1);
                int i2 = WritableComparator.readVInt(b2, s2);

                l1 = WritableUtils.decodeVIntSize(b1[s1]) + i1;
                l2 = WritableUtils.decodeVIntSize(b2[s2]) + i2;

                Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
                comp = TEXT_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);

                int comp2 = fcw1.getFile().toString().compareTo(fcw2.getFile().toString());

                int stop = 1;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return comp;
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
        //serializeTest();

		int status;
		if (args.length > 1) {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "InvertedIndex");
            job.setJarByClass(InvertedIndex.class);
            job.setMapperClass(InvertedIndexMapper.class);
            job.setReducerClass(InvertedIndexReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FileCountWritable.class);
            job.setSortComparatorClass(FileCountComparator.class);
            //job.setOutputKeyClass(InvertedIndexWritable.class);
            //job.setOutputValueClass(NullWritable.class);

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
