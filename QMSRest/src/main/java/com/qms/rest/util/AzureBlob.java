package com.qms.rest.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

public class AzureBlob {

	public static String upload() {
		// TODO Auto-generated method stub

		
		String response = "";
		// TODO Auto-generated method stub
		
				final String connectString = "DefaultEndpointsProtocol=https;AccountName=healthinsightstoragepg;AccountKey=S0QMLYLKWeklifeLRvT1+BJTKQwdgC7YB4YKsykYdgRZbpALqimVoKw41THmVdtq6qdE2oJMhMwyGXz/7B/Hkg==";
				CloudStorageAccount account = null;
				CloudBlobClient blobClient = null;
				CloudBlobContainer blobContainer = null;
				
				try {
					System.out.println("program started");
					account = account.parse(connectString);
					blobClient = account.createCloudBlobClient();
					blobContainer = blobClient.getContainerReference("hipgcontainer");
					blobContainer.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(), new OperationContext());
					
					File sourceFile = new File("C:\\Curis\\import_export\\upload\\NS_FILE_INPUT.csv");          //createTempFile("sampleFile1", ".txt");
					/*System.out.println("Creating a sample file at: " + sourceFile.toString());
					Writer output = new BufferedWriter(new FileWriter(sourceFile));
					output.write("Hello Azure!");
					output.close();*/

					
					CloudBlockBlob blob = blobContainer.getBlockBlobReference("hive/models/noshow/input/123/"+sourceFile.getName());
					blob.uploadFromFile(sourceFile.getAbsolutePath());
					response = "success";
					for (ListBlobItem blobItem : blobContainer.listBlobs()) {
					    System.out.println("URI of blob is: " + blobItem.getUri());
					}
				} catch (InvalidKeyException | URISyntaxException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					response = "failure";
				} catch (StorageException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					response = "failure";
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					response = "failure";
				}
				return response;
	}

	
}
