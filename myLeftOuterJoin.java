package myId.LeftOuterJoin;
//Step 1 import required classes and interfaces
import scala.Tuple2;
import java.util.Iterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

	
public class myLeftOuterJoin {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//Step 2 Read input parameters
		if(args.length < 2) {
			System.err.println("Usage: LeftOuterJoin <users> <transactions>");
			System.exit(1);;
		}
		String usersInputFile = args[0]; //HDFS text file
		String transactionsInputFile = args[1]; //HDFS text file
		System.out.println("users="+ usersInputFile);
		System.out.println("transactions+"+ transactionsInputFile);
		
		//Step 3 create a JavaSparkContext object
		JavaSparkContext ctx = new JavaSparkContext();
		
		//Step 4 create a JavaRDD for users
		JavaRDD<String> users = ctx.textFile(usersInputFile, 1);
		
		//<K2, V2> JavaPairRDD<K2, V2> mapToPair(PairFunction<T, K2, V2> f)
		//Return a new RDD by applying a function to all elements of this RDD.
		//PairFunction<t, K, V> where T => Tuple2<K, V>
		JavaPairRDD<String, Tuple2<String, String>> usersRDD = 
					users.mapToPair(new PairFunction<
						String, //T
						String, //K
						Tuple2<String, String> //V
					>(){
					public Tuple2<String, Tuple2<String, String>> call(String s) {
						String[] userRecord = s.split("\t");
						Tuple2<String, String> location =
							new Tuple2<String, String>("L", userRecord[1]);
					return new Tuple2<String, Tuple2<String, String>>(userRecord[0], location);
					}
				});
		
		//Step 5 create a JavaRDD for transactions
		JavaRDD<String> transactions = ctx.textFile(transactionsInputFile, 1);
		
		//mapToPair
	    //<K2, V2> JavaPairRDD<K2, V2> mapToPair(PairFunction<T, K2, V2> f)
		//Return a new RDD by applying a function to all elements of the RDD.
		//PairFunction<T, K, V>
		//T => Tuple2<K, V>
		JavaPairRDD<String, Tuple2<String, String>> transactionsRDD =
					transactions.mapToPair(new PairFunction<
						String, //T
						String, //K
						Tuple2<String, String> //V
					>(){
					public Tuple2<String, Tuple2<String, String>> call(String s) {
						String[] transactionRecord = s.split("\t");
						Tuple2<String, String> product =
							new Tuple2<String, String>("P", transactionRecord[1]);
						return new Tuple2<String, Tuple2<String, String>>(transactionRecord[2], product);
						}
					});
		
		//Step 6 create a union of the RDDs created in step 4 and step 5
		JavaPairRDD<String, Tuple2<String, String>> allRDD =
				transactionsRDD.union(usersRDD);
		
		//Here we perform a union() on usersRDD and transactionsRdd
		//JavaPairRDD<String, Tuple2<String, String>> allRDD =
		//		usersRDD.union(transactionsRDD);
		
		//Step 7 create a JavaPairRDD (userId, List<T2>) by calling groupBY()
		//group allRDD by userID
		JavaPairRDD<String, Iterable<Tuple2<String, String>>> groupedRDD =
				allRDD.groupByKey();
		//now the groupedRDD entries will be as follows:
		//<userIdi, List[T2("L", location),
						//T2("P", Pi1),
						//T2("P", Pi2), 
						//T2("P", Pi3),...
						//]
		// >
		
		//Step 8 create a productLocationRDD as JavaPairRDD<String, String>
		//PairFlatMapFunction<T, K, V>
		//T => Iterable<Tuple2<K, V>>
		JavaPairRDD<String, String> productLocationsRDD =
				groupedRDD.flatMapToPair(new PairFlatMapFunction<
						Tuple2<String, Iterable<Tuple2<String, String>>>, //T
						String, //K
						String>(){  //V
			public Iterator<Tuple2<String, String>>
				call(Tuple2<String, Iterable<Tuple2<String, String>>> s){
					//String userID = s._1; //NOT needed
					Iterable<Tuple2<String, String>> pairs = s._2;
					String location = "UNKNOWN";
					List<String> products = new ArrayList<String>();
					for(Tuple2<String, String> t2: pairs) {
						if(t2._1.equals("L")) {
							location = t2._2;
						}
						else {
							//t2._1.equals("P")
							products.add(t2._2);
						}
					}
					
					//now emit (K, V) pairs
					List<Tuple2<String, String>> kvList =
						new ArrayList<Tuple2<String, String>>();
					for (String product : products) {
						kvList.add(new Tuple2<String, String>(product, location));
					}
					//Note that edges must be reciprocal; that is, 
					//every {source, destination} edge must have 
					// a corresponding {destination, source}
				return (Iterator<Tuple2<String, String>>) kvList;
				}
		
			});
			
			//Step 9 find all locations for a product;
			//result will be JavaPAirRDD <String, List<String>>
			JavaPairRDD<String, Iterable<String>> productByLocations =
					productLocationsRDD.groupByKey();
			
			//debug3
			List<Tuple2<String, Iterable<String>>> debug3 = productByLocations.collect();
			System.out.println("--- debug3 begin ---");
			for(Tuple2<String, Iterable<String>> t2 : debug3){
				System.out.println("debug3 t2._1=" + t2._1);
				System.out.println("debug3 t2._2+"+ t2._2);
			}
			System.out.println("---debug3 end ---");
			
			//Step 10 finalize output by changing "value" from List<String>
			//to Tuple2<Set<String>, Integer>, where you have a unique
			//set of locations and their count
			JavaPairRDD<String, Tuple2<Set<String>, Integer>> productByUniqueLocations =
					productByLocations.mapValues( 
							new Function<Iterable<String>, //input
							Tuple2<Set<String>, Integer> //output
							>(){
				public Tuple2<Set<String>, Integer> call(Iterable<String> s){
					Set<String> uniqueLocations = new HashSet<String>();
					for(String locations : s) {
						uniqueLocations.add(locations);
					}
					return new Tuple2<Set<String>, Integer>(uniqueLocations, 
															uniqueLocations.size());
				}
				
			});
			
			//Step 11 print the final result RDD
			//debug4
			System.out.println("=== Unique Locations and Counts ===");
			List<Tuple2<String, Tuple2<Set<String>, Integer>>> debug4 =
				productByUniqueLocations.collect();
			System.out.println("---debug4 begins---");
			for(Tuple2<String, Tuple2<Set<String>, Integer>> t2 : debug4) {
				System.out.println("debug4 t2._1="+t2._1);
				System.out.println("debug4 t2._2="+t2._2);
			}
			System.out.println("---debug4 end---");
			
					
	}

}

