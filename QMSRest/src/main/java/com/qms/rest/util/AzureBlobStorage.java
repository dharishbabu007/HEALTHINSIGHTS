package com.qms.rest.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import org.apache.xerces.util.URI;
import org.springframework.web.multipart.MultipartFile;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPermissions;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.qms.rest.model.RestResult;

public class AzureBlobStorage {

	
	public static RestResult azureUploadFile(String connectString, String containerName, 
			String fileId, String baseDir, MultipartFile uploadFile){
		
		if((null == connectString) || (null == containerName) || (null == fileId) || (null == baseDir)) {
			return RestResult.getFailRestResult(" Parameters are null.. ");
		} else {
			CloudStorageAccount account = null;
			CloudBlobClient blobClient = null;
			CloudBlobContainer blobContainer = null;
			try {
				account = account.parse(connectString);
				blobClient = account.createCloudBlobClient();
				blobContainer = blobClient.getContainerReference(containerName);
				blobContainer.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(), new OperationContext());
				
//				File sourceFile = new File("C:\\Curis\\import_export\\upload\\"+ uploadFile.getOriginalFilename());
//				//File sourceFile = new File("C:\\Curis\\Server\\apache-tomcat-8.5.31-windows-x64\\apache-tomcat-8.5.31\\bin\\"+ uploadFile.getOriginalFilename());
//				uploadFile.transferTo(sourceFile);
//				System.out.println(sourceFile.toURI()+" sourceFile.getAbsolutePath() --> " + sourceFile.getAbsolutePath());
				
				/*Multipart file code has to bring here(convert the multipart file to input steram)*/
				//InputStream inputStream = uploadFile.getInputStream();
				String name = uploadFile.getOriginalFilename();
				//long length = uploadFile.getSize();
				System.out.println(" Uploaded file name --> " + name);
				String filePath = baseDir+fileId+"/"+name;
				//String filePath = baseDir+name;
				//URI uri = new URI(filePath);
				System.out.println(" In Bolb file creation path --> " + filePath);
				CloudBlockBlob blob = blobContainer.getBlockBlobReference(filePath);
				blob.upload(uploadFile.getInputStream(), uploadFile.getSize());		
				//blob.uploadFromByteArray(uploadFile.getBytes(), 0, uploadFile.getBytes().length);
				/*String path = sourceFile.getAbsolutePath().replace('\\', File.separatorChar);
				System.out.println("path is:"+path);*/
				//blob.uploadFromFile(sourceFile.getAbsolutePath());
				return RestResult.getSucessRestResult(" Azure file upload success. ");
			} catch (InvalidKeyException | URISyntaxException | IOException | StorageException e ) {
				e.printStackTrace();
				return RestResult.getFailRestResult(e.getMessage());
			} 
		}	
	}
}
