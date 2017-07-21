package mine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import run.Run_EWC;
import util.Parameters;
import fptree.FPTree;
import fptree.FPTreeNode;

public class ParallelFPMineReducer_write_compress extends
		Reducer<IntWritable, Text, Text, Text> {

	// output key
	private Text outputKey = new Text();
	// output value
	private Text outputValue = new Text();

	// Map<uid:support, groupID>
	private Map<String, Integer> mapItemToGroupID = new HashMap<String, Integer>();

	private FPTree fptree;
	private int groupID;

	protected void reduce(IntWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		groupID = key.get();

		Date startCreateFPTree = new Date();

		// 1> create the local fp tree
		fptree = new FPTree();
		Map<String, Integer> mapItemSupport = createFPtree(mapItemToGroupID,
				values);

		// 2> sort the local header table
		fptree.sortHeaderTable(mapItemSupport);
		Date endCreateFPTree = new Date(); // end create local fp tree

		// print groupId statistics *********
		System.out.println("groupID: " + groupID + " statistics ----->>");

		Date startMineFPTree = new Date();
		// 3> mine two frequent itemset
		mineTwoFrequentPatterns(fptree, context);
		Date endMineFPTree = new Date();

		// print create local tree time
		System.out.println("create fptree took: "
				+ (endCreateFPTree.getTime() - startCreateFPTree.getTime())
				/ 1000 + " seconds.");

		// print mine took time
		System.out.println("mine fptree took: "
				+ (endMineFPTree.getTime() - startMineFPTree.getTime()) / 1000
				+ " seconds.");
	}

	private void mineTwoFrequentPatterns(FPTree fptree, Context context)
			throws InterruptedException, IOException {

		Map<String, Integer> mapItemSupportOfSuffix = new HashMap<String, Integer>();
		// process each item in the header table list of the tree in reverse
		// order
		for (int i = fptree.getHeaderItemList().size() - 1; i >= 0; i--) {

			// a) calculate the frequency of each node in the prefixPaths
			// current suffix item
			String suffixItem = fptree.getHeaderItemList().get(i);
			FPTreeNode pathNode = fptree.getMapItemNode().get(suffixItem);

			// find all prefixPath of the suffix item
			while (pathNode != null) {
				// the support of the prefixPath's first node
				int pathSupport = pathNode.getCount();

				// check the node is not just the root
				if (pathNode.getParent().getName() != null) {
					// recursive add all the parent of this node
					FPTreeNode parent = pathNode.getParent();
					while (parent.getName() != null) {
						// calculate the frequency of each node in the
						// prefixPaths
						if (mapItemSupportOfSuffix.get(parent.getName()) == null) {
							mapItemSupportOfSuffix.put(parent.getName(),
									pathSupport);
						} else {
							// make the sum
							int count = mapItemSupportOfSuffix.get(parent
									.getName());
							mapItemSupportOfSuffix.put(parent.getName(), count
									+ pathSupport);
						}
						parent = parent.getParent();
					}
				}
				// look for the next prefixPath
				pathNode = pathNode.getNodeLink();
			}

			// b) context write
			Set<String> itemset = mapItemSupportOfSuffix.keySet();
			if (itemset.size() > 0) {
				// suffix item
				String[] suffixItemSplit = suffixItem.split(":");
				String suffixItemName = suffixItemSplit[0];
				float suffixItemSupport = Integer.parseInt(suffixItemSplit[1]);

				StringBuilder buff = new StringBuilder(itemset.size());

				// for each prefixPaths item of the current suffix item
				for (String prefixItem : itemset) {

					// prefix item
					String[] prefixItemSplit = prefixItem.split(":");
					String prefixItemName = prefixItemSplit[0];
					float prefixItemSupport = Integer
							.parseInt(prefixItemSplit[1]);

					// two itemset support
					float twoItemsetSupport = mapItemSupportOfSuffix
							.get(prefixItem);

					// calculate the two itemset's weight
					float weight = (float) (Math
							.round((twoItemsetSupport / (prefixItemSupport
									+ suffixItemSupport - twoItemsetSupport)) * 100)) / 100;

					buff.append(prefixItemName + ',' + weight + ' ');
				}

				outputKey.set(suffixItemName);
				outputValue.set(buff.toString());
				context.write(outputKey, outputValue);
			}

			mapItemSupportOfSuffix.clear();
		}
	} // end mineTwoFrequentPatterns()

	private Map<String, Integer> createFPtree(
			Map<String, Integer> mapItemToGroupID, Iterable<Text> values) {
		// Map <item, item support> based on group
		Map<String, Integer> mapItemSupport = new HashMap<String, Integer>();
		// for each local transaction
		for (Text value : values) {
			List<String> transaction = new ArrayList<String>();
			StringTokenizer itemtoken = new StringTokenizer(value.toString());

			while (itemtoken.hasMoreTokens()) {
				String item = itemtoken.nextToken();
				// check the item is in the group list
				if (mapItemToGroupID.get(item) == groupID) {
					// calculate the item and its support
					Integer count = mapItemSupport.get(item);
					if (count == null) {
						mapItemSupport.put(item, 1);
					} else {
						// increase the count
						mapItemSupport.put(item, ++count);
					}
				}
				transaction.add(item);
			}
			// insert the transaction to the fp tree
			fptree.addTransaction(transaction, mapItemToGroupID, groupID);
		}
		return mapItemSupport;
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		Parameters params = new Parameters(context.getConfiguration().get(
				Run_EWC.EDGE_WEIGHT_PARAMETERS, ""));

		// read the distributed cache fList file
		mapItemToGroupID = Run_EWC.readDistributeCacheFListFile(context.getConfiguration());

		// for eclipse local test
		// mapItemToGroupID=Run_EWC.readGList(params);
	}
}
