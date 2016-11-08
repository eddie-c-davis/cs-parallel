import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Builds an inverted index: each word followed by files it was found in.
 * 
 * 
 */
public class InvertedIndex {
    public static class FileCountWritable implements Writable {
        private String file = "";
        private Integer count = 0;

        public  FileCountWritable(String f, Integer c) {
            set(f, c);
        }

        public  FileCountWritable() {
            this("", 0);
        }

        public void set(String f, Integer c) {
            file = f;
            count = c;
        }

        public String getFile() {
            return file;
        }

        public Integer getCount() {
            return count;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(file);
            out.writeInt(count);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            file = in.readUTF();
            count = in.readInt();
        }

        @Override
        public boolean equals(Object obj) {
            FileCountWritable other = (FileCountWritable) obj;
            return (file.equals(other.getFile()) && count.equals(other.getCount()));
        }

        @Override
        public String toString() {
            return count + " " + file;
        }
    }

	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
		private final static String pattern = " , .;:'\"&!?-_\n\t12345678910[]{}<>\\`~|=^()@#$%^*/+-";
		private final static Text word = new Text();
		private final static Text location = new Text();

        private static HashMap<String, HashMap<String, Integer>> fileMap; // = new HashMap<>();

        public void setup(Context context) {
            fileMap = new HashMap<>();
        }

		public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            String line = val.toString().trim();
            if (!line.isEmpty()) {
                FileSplit fileSplit = (FileSplit) context.getInputSplit();
                String fileName = fileSplit.getPath().getName();
                //location.set(fileName);

                if (!fileMap.containsKey(fileName)) {
                    fileMap.put(fileName, new HashMap<String, Integer>());
                }

                HashMap<String, Integer> wordMap = fileMap.get(fileName);

                StringTokenizer itr = new StringTokenizer(line.toLowerCase(), pattern);
                while (itr.hasMoreTokens()) {
                    String token = itr.nextToken();
                    if (!wordMap.containsKey(token)) {
                        wordMap.put(token, 0);
                    }

                    wordMap.put(token, wordMap.get(token) + 1);

                    //word.set(token);
                    //context.write(word, location);
                }
            }
		}

        public void cleanup(Context context) throws IOException, InterruptedException {
            emit(context);
        }

        private void emit(Context context) throws IOException, InterruptedException {
            for (String fileName : fileMap.keySet()) {
                location.set(fileName);
                //System.out.println(fileName + ":");
                HashMap<String, Integer> wordMap = fileMap.get(fileName);
                for (String token : wordMap.keySet()) {
                    word.set(token);
                    Integer count = wordMap.get(token);
                    //System.out.println(token + " " + count.toString());
                    context.write(word, location);
                }

                wordMap.clear();
            }

            fileMap.clear();
        }
	}

	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			boolean first = true;
			Iterator<Text> iter = values.iterator();
			StringBuilder toReturn = new StringBuilder();

            while (iter.hasNext()) {
				if (!first) {
					toReturn.append(", ");
                }

				first = false;

                String fileName = iter.next().toString();
				toReturn.append(fileName);
			}

			context.write(key, new Text(toReturn.toString()));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		int status = 0;
		if (args.length > 1) {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "InvertedIndex");
            job.setJarByClass(InvertedIndex.class);
            job.setMapperClass(InvertedIndexMapper.class);
            job.setReducerClass(InvertedIndexReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            status = job.waitForCompletion(true) ? 0 : 1;
        } else {
            System.out.println("Usage: InvertedIndex <input path> <output path>");
            status = 1;
        }

        System.exit(status);
	}
}
