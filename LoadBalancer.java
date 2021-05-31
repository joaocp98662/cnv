package loadbalancer;

import pt.ulisboa.tecnico.cnv.server.ServerArgumentParser;

import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.Iterator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.net.InetSocketAddress;
import java.net.HttpURLConnection;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;

//Zé adicionou
import com.google.common.collect.Multimap;
import com.google.common.collect.ArrayListMultimap;

import database.DataBase;

public class LoadBalancer {

    static AmazonEC2      ec2;
    static AmazonCloudWatch cloudWatch;

	static ServerArgumentParser sap = null;

	// Multimap declaration
	private static Multimap<String, String> instancesMap = ArrayListMultimap.create();
	static Semaphore sem = new Semaphore(1);
	//private volatile boolean startingNewInstance = false;

	//private static HashMap<String, String> instancesMap = new HashMap<String, String>();

	public static void main(final String[] args) throws Exception {

		try {
			// Get user-provided flags.
			LoadBalancer.sap = new ServerArgumentParser(args);
		}
		catch(Exception e) {
			System.out.println(e);
			return;
		}

		System.out.println("> Finished parsing Server args.");

		//final HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 8000), 0);

		final HttpServer server = HttpServer.create(new InetSocketAddress(LoadBalancer.sap.getServerAddress(), LoadBalancer.sap.getServerPort()), 0);

		server.createContext("/scan", new MyHandler());

		// be aware! infinite pool of threads!
		server.setExecutor(Executors.newCachedThreadPool());
		server.start();

		System.out.println(server.getAddress().toString());
	}


	public static InputStream manageWorkLoad(String query, String[] args) throws Exception {

		init();

		try {

			JSONArray metrics = DataBase.getDataForPrediction("metrics", args[1], args[17]);
			
			List<Double> x = new ArrayList<Double>();
			List<Double> y = new ArrayList<Double>();

			Iterator<JSONObject> metricsIterator =  metrics.iterator();

			 while(metricsIterator.hasNext()) {

		        JSONObject item = metricsIterator.next();
		        x.add(item.get("area"));
		        y.add(item.get("instr_count"));
	
			}

			System.out.println("X - " + x);
			System.out.println("Y - " + y);

			// JsonArray x = (JsonArray) metrics.get("area");
			// JsonArray y = (JsonArray) metrics.get("instr_count");

			// Double area = (args[7] - args[5]) * (args[11] - args[9]);

			// LinearRegression lr = new LinearRegression(x.getDoubleArray(), y.getDoubleArray());

			// System.out.println("Prediction - " - lr.predict(area));

			// Get imageID

			Filter filterImageByName = new Filter("name");
	        filterImageByName.withValues("CNV-Project");
			
        	DescribeImagesRequest describesImagesRequest = new DescribeImagesRequest();
        	describesImagesRequest.withFilters(filterImageByName);

        	DescribeImagesResult describeImagesResult = ec2.describeImages(describesImagesRequest);

			List<Image> images = describeImagesResult.getImages();
			String imageID = images.get(0).getImageId();
			
			System.out.println("\nAMI ID - " + imageID);

			sem.acquire();

			// Get active instances
			Set<Instance> instances = LoadBalancer.listRunningInstancesByImageID(imageID);

			//System.out-println(instances.);

			String instanceIP = null;

			// while(startingNewInstance) {
			// 	System.out.println("ENTROU AQUI!!!!!!!!!");
			// 	Thread.sleep(200);
			// }

			// Check if there are no instances running
			if(instances.isEmpty()) {

				//startingNewInstance = true;

				// Start an instance - Auto Scaler
				String instanceID = LoadBalancer.startInstance(imageID);
				System.out.println("Starting a new instance with ID " + instanceID + " ...");

				// Wating for running instance to run
				while(checkInstanceRunning(instanceID) == false) {
					Thread.sleep(500);					
				}

				//startingNewInstance = false;
				sem.release();

				//Obtain instance IP address
				instanceIP = LoadBalancer.getInstanceIP(instanceID);
				System.out.println("Instance " + instanceID + "running with IP address " + instanceIP);				


			} else { /* instances running in AWS */

				sem.release();

				for (Instance inst : instances) {

					if(instancesMap.get(inst.getPublicIpAddress()).toString().equals("[]")) {
						instanceIP = inst.getPublicIpAddress();
						System.out.println("2º Condition - Sent request to a free instance with IP " + instanceIP);
						break;
					}
				}

				// Enters if didn't find any free instance
				if(instanceIP == null) {

					System.out.println("ENTROU CARAGO!!!");

					// JSONObject metrics = DataBase.getDataForPrediction(tableName, args[1], args[17]);

					// JsonArray x = (JsonArray) metrics.get("area");
					// JsonArray y = (JsonArray) metrics.get("instr_count");

					// Double area = (args[7] - args[5]) * (args[11] - args[9]);

					// LinearRegression lr = new LinearRegression(x.getDoubleArray(), y.getDoubleArray());

					// System.out.println("Prediction - " - lr.predict(area));

					// choose the right instance
					boolean firstTime = true;
					int countJobs = 0;

					for (Object key : instancesMap.keys()) { 

						if(firstTime) {
							System.out.println("ENTROU 2!!!");
							countJobs = instancesMap.keys().count(key);
							instanceIP = key.toString();
							firstTime = false;
						
						} else {

							if(instancesMap.keys().count(key) < countJobs) {
								System.out.println("3ª Condition ... ");
								instanceIP = key.toString();
							}
						}	
					}
				}
			}

			System.out.println("Request will be send to " + instanceIP);

			// Updates Map only if is not a new Instance
			instancesMap.put(instanceIP, query);

			System.out.println("BEFORE " + instancesMap);

			InputStream response = LoadBalancer.sendRequestToInstance(instanceIP, query);

			// Updates instanceMap of queries/instances running			
			instancesMap.remove(instanceIP, query);

			System.out.println("AFTER " + instancesMap);

			return response;


		} catch (AmazonServiceException ase) {
            // System.out.println("Caught Exception: " + ase.getMessage());
            // System.out.println("Reponse Status Code: " + ase.getStatusCode());
            // System.out.println("Error Code: " + ase.getErrorCode());
            // System.out.println("Request ID: " + ase.getRequestId());
            //instancesMap.remove(instanceIP, query); TOOOOOOOOOOOOOOO CHECKKKKKKKKKKKKKKK!!!!!!!
            throw new AmazonServiceException(ase.getMessage());
        }
		catch (IOException e) {
			//instancesMap.remove(instanceIP, query); TOOOOOOOOOOOOOOO CHECKKKKKKKKKKKKKKK!!!!!!! 
			throw new IOException(e.getMessage());
        }
        
	}

	public static InputStream sendRequestToInstance(String instanceIP, String query) throws IOException {

		String url = "http://" + instanceIP + ":8000/scan?" + query;
		
		System.out.println(url);
		
		// Send an HTTP Request with the given query to the instance's IP address and receives its response
		HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
		//URLConnection connection = new URL(url).openConnection();
		
		connection.setRequestMethod("GET");
		//connection.setRequestProperty("Accept-Charset", charset);

		// Get the response code 
		int statusCode = connection.getResponseCode();
		
		InputStream response = null;

		if (statusCode >= HttpURLConnection.HTTP_BAD_REQUEST) {
		    throw new IOException("Error connecting to " + url + '(' + statusCode + ")");
		}

		System.out.println("Request status: " + statusCode);
		return connection.getInputStream();
	}

    private static void init() throws Exception {

        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

      	ec2 = AmazonEC2ClientBuilder.standard().withRegion("us-east-1").withCredentials(new AWSStaticCredentialsProvider(credentials)).build();

		cloudWatch = AmazonCloudWatchClientBuilder.standard().withRegion("us-east-1").withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
    }


	static class MyHandler implements HttpHandler {
		@Override
		public void handle(final HttpExchange t) throws IOException {

			// Get the query.
			final String query = t.getRequestURI().getQuery();

			System.out.println("> Query:\t" + query);

			// Break it down into String[].
			final String[] params = query.split("&");

			/*
			for(String p: params) {
				System.out.println(p);
			}
			*/

			// Store as if it was a direct call to SolverMain.
			final ArrayList<String> newArgs = new ArrayList<>();
			for (final String p : params) {
				final String[] splitParam = p.split("=");

				if(splitParam[0].equals("i")) {
					splitParam[1] = LoadBalancer.sap.getMapsDirectory() + "/" + splitParam[1];
				}

				newArgs.add("-" + splitParam[0]);
				newArgs.add(splitParam[1]);

				/*
				System.out.println("splitParam[0]: " + splitParam[0]);
				System.out.println("splitParam[1]: " + splitParam[1]);
				*/
			}

			if(sap.isDebugging()) {
				newArgs.add("-d");
			}


			// Store from ArrayList into regular String[].
			final String[] args = new String[newArgs.size()];
			int i = 0;
			for(String arg: newArgs) {
				args[i] = arg;
				i++;
			}

			try {

				InputStream response = LoadBalancer.manageWorkLoad(query, newArgs);

				// Send response to browser.
				final Headers hdrs = t.getResponseHeaders();

				hdrs.add("Content-Type", "image/png");

				hdrs.add("Access-Control-Allow-Origin", "*");
				hdrs.add("Access-Control-Allow-Credentials", "true");
				hdrs.add("Access-Control-Allow-Methods", "POST, GET, HEAD, OPTIONS");
				hdrs.add("Access-Control-Allow-Headers", "Origin, Accept, X-Requested-With, Content-Type, Access-Control-Request-Method, Access-Control-Request-Headers");

	            final byte[] buffer = new byte[response.available() == 0 ? 1024 : response.available()];
	            System.out.println("buffer size=" + buffer.length);


	            final ByteArrayOutputStream baos = new ByteArrayOutputStream(buffer.length);
	            int length;

	            while ((length = response.read(buffer, 0, buffer.length)) >= 0) {
	                baos.write(buffer, 0, length);
	            }

	            t.sendResponseHeaders(200, baos.size());

	            final OutputStream os = t.getResponseBody();

	            baos.writeTo(os);	
				
				// t.sendResponseHeaders(200, response.available());

				// final OutputStream os = t.getResponseBody();

				// // Copy response to OutputStream
				// IOUtils.copy(response, os);		

				//os.close();

			} catch(Exception e) {
				System.out.println(e.getMessage());
			}

		}
	}	

	public static synchronized String startInstance(String imageID) throws Exception {

        //init();

        /*
         * Amazon EC2
         *
         * The AWS EC2 client allows you to create, delete, and administer
         * instances programmatically.
         *
         * In this sample, we use an EC2 client to get a list of all the
         * availability zones, and all instances sorted by reservation id, then 
         * create an instance, list existing instances again, wait a minute and 
         * the terminate the started instance.
         */

        String newInstanceID = null;

        try {

            RunInstancesRequest runInstancesRequest = new RunInstancesRequest();

            runInstancesRequest.withImageId(imageID)
                               .withInstanceType("t2.micro")
                               .withMinCount(1)
                               .withMaxCount(1)
                               .withKeyName("CNV-project")
                               .withSecurityGroups("CNV-project");
                               
            RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);
               
            newInstanceID = runInstancesResult.getReservation().getInstances().get(0).getInstanceId();

            // Wait 1 second to obtain the instance's public IP
            // Thread.sleep(1000);
            // String newInstanceIP = runInstancesResult.getReservation().getInstances().get(0).getPublicIpAddress();

            // return runInstancesResult;
            
        } catch (AmazonServiceException ase) {
                System.out.println("Caught Exception: " + ase.getMessage());
                System.out.println("Reponse Status Code: " + ase.getStatusCode());
                System.out.println("Error Code: " + ase.getErrorCode());
                System.out.println("Request ID: " + ase.getRequestId());
        }

        return newInstanceID;

	}

	public static String getInstanceIP(String instanceID) throws Exception {

		//init();

		String instanceIP = null;

		try {

	        Filter filterEC2ByID = new Filter("instance-id");
	        filterEC2ByID.withValues(instanceID);
	        Filter filterEC2ByState = new Filter("instance-state-name");
	        filterEC2ByState.withValues("running");

	        DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest();
	        describeInstancesRequest.withFilters(filterEC2ByID, filterEC2ByState);

			DescribeInstancesResult describeInstancesResult = ec2.describeInstances(describeInstancesRequest);			

			Reservation reservation = describeInstancesResult.getReservations().get(0);

			instanceIP = reservation.getInstances().get(0).getPublicIpAddress();

        } catch (AmazonServiceException ase) {
	        System.out.println("Caught Exception: " + ase.getMessage());
	        System.out.println("Reponse Status Code: " + ase.getStatusCode());
	        System.out.println("Error Code: " + ase.getErrorCode());
	        System.out.println("Request ID: " + ase.getRequestId());
        }

        return instanceIP;
	}

	public static boolean checkInstanceExists(String instanceIP) throws Exception {

		//init();

		boolean instanceExists = false;

		try {

	        Filter filterEC2ByIP = new Filter("ip-address");
	        filterEC2ByIP.withValues(instanceIP);

			DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest();
			describeInstancesRequest.withFilters(filterEC2ByIP);

			DescribeInstancesResult describeInstancesResult = ec2.describeInstances(describeInstancesRequest);			

			Reservation reservation = describeInstancesResult.getReservations().get(0);

			if (reservation != null) {
				instanceExists = true;
			}

        } catch (AmazonServiceException ase) {
                System.out.println("Caught Exception: " + ase.getMessage());
                System.out.println("Reponse Status Code: " + ase.getStatusCode());
                System.out.println("Error Code: " + ase.getErrorCode());
                System.out.println("Request ID: " + ase.getRequestId());
        }

        return instanceExists;
	}

	public static boolean checkInstanceRunning(String instanceID) throws Exception {

		//init();

		boolean instanceRunning = false;

		try {

	        Filter filterEC2ByID = new Filter("instance-id");
	        filterEC2ByID.withValues(instanceID);

            Filter filterEC2ByState = new Filter("instance-state-name");
            filterEC2ByState.withValues("running");

	        DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest();
	        describeInstancesRequest.withFilters(filterEC2ByID, filterEC2ByState);

			DescribeInstancesResult describeInstancesResult = ec2.describeInstances(describeInstancesRequest);

			List<Reservation> reservations = describeInstancesResult.getReservations(); 

			if (reservations.size() > 0) {
				instanceRunning = true;
			}

        } catch (AmazonServiceException ase) {
                System.out.println("Caught Exception: " + ase.getMessage());
                System.out.println("Reponse Status Code: " + ase.getStatusCode());
                System.out.println("Error Code: " + ase.getErrorCode());
                System.out.println("Request ID: " + ase.getRequestId());
        }

        return instanceRunning;
	}	

	public static Set<Instance> listRunningInstancesByImageID(String imageID) throws Exception {

		//init();

		Set<Instance> instances = new HashSet<Instance>();

        try {         	

            //Create Filters to use to find running instances
            Filter filterEC2ByImageId = new Filter("image-id");
            filterEC2ByImageId.withValues(imageID);

            Filter filterEC2ByinstanceType = new Filter("instance-type");
            filterEC2ByinstanceType.withValues("t2.micro");

            Filter filterEC2ByState = new Filter("instance-state-name");
            filterEC2ByState.withValues("running");

			DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest();
			describeInstancesRequest.withFilters(filterEC2ByImageId, filterEC2ByinstanceType, filterEC2ByState);

			DescribeInstancesResult describeInstancesResult = ec2.describeInstances(describeInstancesRequest);

            List<Reservation> reservations = describeInstancesResult.getReservations();

            for (Reservation reservation : reservations) {
                instances.addAll(reservation.getInstances());
            }

            System.out.println("You have " + instances.size() + " Amazon EC2 instance(s) running.");            

            // long offsetInMilliseconds = 1000 * 60 * 10;
            // Dimension instanceDimension = new Dimension();
            // instanceDimension.setName("InstanceId");
            // List<Dimension> dims = new ArrayList<Dimension>();
            // dims.add(instanceDimension);

            // for (Instance instance : instances) {
            //     String name = instance.getInstanceId();
            //     String state = instance.getState().getName();
            //     if (state.equals("running")) { 
            //         System.out.println("running instance id = " + name);
            //         instanceDimension.setValue(name);
            //         GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
            //         .withStartTime(new Date(new Date().getTime() - offsetInMilliseconds))
            //         .withNamespace("AWS/EC2")
            //         .withPeriod(60)
            //         .withMetricName("CPUUtilization")
            //         .withStatistics("Average")
            //         .withDimensions(instanceDimension)
            //         .withEndTime(new Date());
            //          GetMetricStatisticsResult getMetricStatisticsResult = 
            //              cloudWatch.getMetricStatistics(request);
            //          List<Datapoint> datapoints = getMetricStatisticsResult.getDatapoints();
            //          for (Datapoint dp : datapoints) {
            //            System.out.println(" CPU utilization for instance " + name +
            //                " = " + dp.getAverage());
            //          }
            //      }
            //      else {
            //         System.out.println("instance id = " + name);
            //      }
            //     System.out.println("Instance State : " + state +".");
            // }

        } catch (AmazonServiceException ase) {
                System.out.println("Caught Exception: " + ase.getMessage());
                System.out.println("Reponse Status Code: " + ase.getStatusCode());
                System.out.println("Error Code: " + ase.getErrorCode());
                System.out.println("Request ID: " + ase.getRequestId());
        }

        return instances;
    }

    public static void terminateInstance(String instanceID) throws Exception {

    	//init();

    	try {

	        TerminateInstancesRequest termInstanceReq = new TerminateInstancesRequest();
	        termInstanceReq.withInstanceIds(instanceID);
	        ec2.terminateInstances(termInstanceReq);

        } catch (AmazonServiceException ase) {
                System.out.println("Caught Exception: " + ase.getMessage());
                System.out.println("Reponse Status Code: " + ase.getStatusCode());
                System.out.println("Error Code: " + ase.getErrorCode());
                System.out.println("Request ID: " + ase.getRequestId());
        }
    }
}
