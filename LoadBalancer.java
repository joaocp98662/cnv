package loadbalancer;

import pt.ulisboa.tecnico.cnv.server.ServerArgumentParser;

import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Executors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.InetSocketAddress;
import java.net.HttpURLConnection;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.apache.commons.io.IOUtils;

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

import database.DataBase;

public class LoadBalancer {

    static AmazonEC2      ec2;
    static AmazonCloudWatch cloudWatch;

	static ServerArgumentParser sap = null;

	private static HashMap<String, String> instancesMap = new HashMap<String, String>();

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


	public static synchronized InputStream manageWorkLoad(String query) throws Exception {

		init();

		try {

			String loadBalancerInstanceID = "i-0c65f95829151ddbb";

			System.out.println("Entrou 0");
			// Get imageID

			/*
			Filter filterImageByName = new Filter("name");
	        	filterImageByName.withValues("CNV-project");
			
	        	DescribeImagesRequest describesImagesRequest = new DescribeImagesRequest();
	        	DescribeImagesResult describeImagesResult = ec2.describeImages(describesImagesRequest);

			List<Image> images = describeImagesResult.getImages();
			String imageID = images.get(0).getImageId();
			*/
			String imageID = "ami-0c52f2cecaaebc721";
			System.out.println(imageID);

			// Get active instances
			Set<Instance> instances = LoadBalancer.listRunningInstancesByImageID(imageID, loadBalancerInstanceID);

			String instanceIP = null;

			System.out.println("ENTROU 1");
			// Check if there are no instances running
			if(instances.isEmpty()) {
				System.out.println("ENTROU 2");
				// Start an instance - Auto Scaler
				String instanceID = LoadBalancer.startInstance(imageID);

				//Obtain instance IP address
				instanceIP = LoadBalancer.getInstanceIP(instanceID);

				// Wating for running instance to run
				while((LoadBalancer.listRunningInstancesByImageID(imageID)).isEmpty()) {
					Thread.sleep(500);
				}


			} else {

				// check if there are instances running that are free (not running queries)
				if(instancesMap.isEmpty()) {

					System.out.println("ENTROU 3");
					// Start an instance - Auto Scaler
					String instanceID = LoadBalancer.startInstance(imageID);

					//Obtain instance IP address
					instanceIP = LoadBalancer.getInstanceIP(instanceID);

					// Wating for running instance to run
					while((LoadBalancer.listRunningInstancesByImageID(imageID)).isEmpty()) {
						Thread.sleep(500);
					}


				} else {

					System.out.println("ENTROU 4");

				}
			}

			InputStream response = LoadBalancer.sendRequestToInstance(instanceIP, query);

			// Updates instanceMap
			instancesMap.remove(query);

			return response;


		} catch (AmazonServiceException ase) {
            // System.out.println("Caught Exception: " + ase.getMessage());
            // System.out.println("Reponse Status Code: " + ase.getStatusCode());
            // System.out.println("Error Code: " + ase.getErrorCode());
            // System.out.println("Request ID: " + ase.getRequestId());
            throw new AmazonServiceException(ase.getMessage());
        }
		catch (IOException e) {
			throw new IOException(e.getMessage());
        }
        
	}

	public static InputStream sendRequestToInstance(String instanceIP, String query) throws IOException {

		String url = "http://" + instanceIP + ":8000/scan?" + query;

		// Send an HTTP Request with the given query to the instance's IP address and receives its response
		HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
		connection.setRequestMethod("GET");
		//connection.setRequestProperty("Accept-Charset", charset);

		// Get the response code 
		int statusCode = connection.getResponseCode();

		InputStream response = null;

		if (statusCode >= HttpURLConnection.HTTP_BAD_REQUEST) {
		    throw new IOException("Error connecting to " + url + '(' + statusCode + ")");
		}

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
				System.out.println("TESTE TESTE TESTE");
				InputStream response = LoadBalancer.manageWorkLoad(query);

				System.out.println("TESTE TESTE TESTE AFTER");

				// Send response to browser.
				final Headers hdrs = t.getResponseHeaders();

				hdrs.add("Content-Type", "image/png");

				hdrs.add("Access-Control-Allow-Origin", "*");
				hdrs.add("Access-Control-Allow-Credentials", "true");
				hdrs.add("Access-Control-Allow-Methods", "POST, GET, HEAD, OPTIONS");
				hdrs.add("Access-Control-Allow-Headers", "Origin, Accept, X-Requested-With, Content-Type, Access-Control-Request-Method, Access-Control-Request-Headers");
				
				t.sendResponseHeaders(200, response.available());

				final OutputStream os = t.getResponseBody();

				// Copy response to OutputStream
				IOUtils.copy(response, os);		

				os.close();

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

            System.out.println("Starting a new instance.");
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

	public static synchronized Set<Instance> listRunningInstancesByImageID(String imageID, String loadBalancerInstanceID) throws Exception {

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

            Filter filterLBInstance = new Filter("instance-id");
            filterLBInstance.withValues("!" + loadBalancerInstanceID);

			DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest();
			describeInstancesRequest.withFilters(filterEC2ByImageId, filterEC2ByinstanceType, filterEC2ByState, filterLBInstance);

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
