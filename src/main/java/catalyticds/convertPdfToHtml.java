package catalyticds;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.jpedal.examples.html.PDFtoHTML5Converter;
import org.jpedal.render.output.IDRViewerOptions;
import org.jpedal.render.output.html.HTMLConversionOptions;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

public class convertPdfToHtml implements RequestHandler<S3EventNotification, Object> {

	public Object pdftohtml(S3EventNotification input, Context context) {

		long startTime = System.currentTimeMillis();
		LambdaLogger logger = context.getLogger();
		AmazonS3Client s3Client = new AmazonS3Client();
		String destbucket = "pdfconverted";
		String topicArn = "arn:aws:sns:us-west-2:526877812830:PDFConverted";
		String rootsubfoldertocreate = "";
		String keyname = "";
		String bucketname = "";
		// Write log to CloudWatch using LambdaLogger.

		logger.log("pdftohtml fired for " + input.toJson());

		for (S3EventNotificationRecord s3record : input.getRecords())
		{
			//for now assume the event is good and only one
			logger.log("S3EventNotificationRecord fired for event Name " + s3record.getEventName());
			logger.log("S3EventNotificationRecord fired for event time" + s3record.getEventTime().toDateTime());
			logger.log("S3EventNotificationRecord fired for s3 object bucket" + s3record.getS3().getBucket().getName());
			logger.log("S3EventNotificationRecord fired for s3 object key" + s3record.getS3().getObject().getKey());

			keyname = s3record.getS3().getObject().getKey().replaceAll("\\+", " ");
			bucketname = s3record.getS3().getBucket().getName().replaceAll("\\+", " ");

			rootsubfoldertocreate = keyname.substring(0, keyname.lastIndexOf('.'));
			File localFile = new File("/tmp/" + keyname);

			ObjectMetadata s3meta = s3Client.getObject(new GetObjectRequest(bucketname, keyname), localFile);

			HTMLConversionOptions conversionOptions = new HTMLConversionOptions();//Set conversion options here
			conversionOptions.setDisableComments(true);
			//ContentOptions contentOptions = new ContentOptions();//Set content options here
			IDRViewerOptions viewerOptions = new IDRViewerOptions();//Set viewer options here
			viewerOptions.setEnableToolBarPDFDownload(true);

			String fileOutputPath = "/tmp";
			File outputFile = new File(fileOutputPath);

			System.out.println("Start convert=" + System.currentTimeMillis());
			Path path = Paths.get(localFile.getAbsolutePath());
			try
			{
				byte[] data = Files.readAllBytes(path);
				PDFtoHTML5Converter converter = new PDFtoHTML5Converter(data, outputFile, conversionOptions,
						viewerOptions);
				converter.convert();

				//assume files are done now and upload to s3
				uploadfilestoS3(outputFile, s3Client, destbucket, rootsubfoldertocreate);

				//publish message with url to index.html
				long endTime = System.currentTimeMillis();
				long totalTime = (endTime - startTime);
				int timeinseconds = (int) (totalTime / 1000) % 60;

				String urltopdf = "https://s3-us-west-2.amazonaws.com/" + destbucket + "/" + rootsubfoldertocreate + "/index.html";

				String msg = "PDF " + s3record.getS3().getObject().getKey() + " was converted to html5 and uploaded to s3 in   "
						+ timeinseconds + "seconds. Click the link to view " + urltopdf;

				//delete pdf from ingest bucket
				s3Client.deleteObject(new DeleteObjectRequest(bucketname, keyname));

				AmazonSNSClient snsClient = new AmazonSNSClient();
				snsClient.setRegion(Region.getRegion(Regions.US_WEST_2));
				PublishRequest publishRequest = new PublishRequest(topicArn, msg);
				PublishResult publishResult = snsClient.publish(publishRequest);
				//print MessageId of message published to SNS topic
				logger.log("AmazonSNSClient MessageId - " + publishResult.getMessageId());

			} catch (Exception ex)
			{
				logger.log("Exception in pdftohtml S3EventNotificationRecord at" + s3record.getEventTime().toDateTime());
				logger.log(ex.getMessage());
				logger.log(ex.getCause().toString());
				logger.log("Exception in pdftohtml PDFIN was=" + s3record.getS3().getObject().getKey() + " cleanname=" + keyname);
				logger.log(s3record.toString());
			}

			break;
		}

		return null;
	}

	//WMS TODO ADD s3 code
	public static void uploadfilestoS3(File dir, AmazonS3Client s3Client, String destbucket, String rootsubfolder) {
		try {
			File[] files = dir.listFiles();
			for (File file : files) {
				if (file.isDirectory()) {
					System.out.println("bucketdirectory:" + file.getAbsolutePath());
					uploadfilestoS3(file, s3Client, destbucket, rootsubfolder);
				} else {

					String news3key = file.getAbsolutePath();
					news3key = news3key.replace("/tmp/output/", rootsubfolder + "/");
					news3key = news3key.replace("/tmp/", rootsubfolder + "/");
					//TODO:  see if u don't have ext on file shouldn't upload....
					if (news3key.contains("."))
					{
						PutObjectRequest por = new PutObjectRequest(destbucket, news3key, file);
						por.setCannedAcl(CannedAccessControlList.PublicRead);
						s3Client.putObject(por);
						System.out.println(" PutObjectResult   complete bucket = " + destbucket + " key=" + news3key);
					}

				}
			}
		} catch (Exception e) {
			System.out.println("uploadfilestoS3 exception=" + e.getMessage());
		}
	}

	public Object handleRequest(S3EventNotification input, Context context) {
		// TODO Auto-generated method stub

		return null;
	}
}
