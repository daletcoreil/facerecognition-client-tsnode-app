import {
    AuthApi,
    Token,
    JobMediatorInput,
    Locator,
    ExtractFacesInput,
	ClusterFacesInput,
    Job,
    MediatorJob,
    JobsApi,
    JobMediatorStatus,
    JobsApiApiKeys,
	ClusterFacesOutput,
	ExtractFacesOutput,
	SearchFacesInput,
	SearchFacesOutput,
    FaceRecognitionApi,
    FaceRecognitionApiApiKeys,
    FaceExtractionCollection,
    Face,
    ClusterCollection
} from 'facerecognition-client'

import fs, {ReadStream} from 'fs'
import * as AWS from 'aws-sdk';


const appConfigFile: string = <string> process.env.APP_CONFIG_FILE;
const appConfig = JSON.parse(fs.readFileSync(appConfigFile).toString());

const client: string = appConfig['clientKey'];
const secret: string = appConfig['clientSecret'];

const projectServiceId: string = appConfig['projectServiceId'];
const aws_access_key_id: string = appConfig['aws_access_key_id'];
const aws_secret_access_key: string = appConfig['aws_secret_access_key'];
const aws_session_token: string = appConfig['aws_session_token'];
const effort: any = appConfig['effort'];
const inputImageKey: string = appConfig['inputImage']


AWS.config.update({accessKeyId: aws_access_key_id, secretAccessKey: aws_secret_access_key});
AWS.config.region = appConfig['bucketRegion'];

const credentials = new AWS.Credentials(aws_access_key_id, aws_secret_access_key, aws_session_token);
const s3 = new AWS.S3({
    apiVersion: '2006-03-01',
	signatureVersion: 'v4',
    s3ForcePathStyle: true,
	credentials: credentials
});

const inputMediaFile: {
    folder: string,
    name: string,
    duration: number
} = {
    "folder": appConfig['localPath'],
    "name": appConfig['inputFile'],
    "duration": 30
};

const inputImageFile: {
    folder: string,
    name: string
} = {
    "folder": appConfig['localPath'],
    "name": appConfig['inputImage']
};


const s3Bucket: string = appConfig['bucketName'];
const baseUrl: string = appConfig['host'];
const authApi: AuthApi = new AuthApi(baseUrl);
const jobsApi: JobsApi = new JobsApi(baseUrl);
const faceRecognitionApi: FaceRecognitionApi = new FaceRecognitionApi(baseUrl);


const createMediatorExtractFacesJobInput = (s3InputSignedUrl: string): JobMediatorInput => {

    let inputFile: Locator  = new Locator();
    inputFile.awsS3Bucket   = s3Bucket;
    inputFile.awsS3Key      = inputMediaFile.name;
    inputFile.httpEndpoint  = s3InputSignedUrl;

    let jobInput: ExtractFacesInput = new ExtractFacesInput();
    jobInput.jobInputType   = ExtractFacesInput.name;
    jobInput.video      	= inputFile;
	if (effort) {
		jobInput.effort = effort;
	}

    let job: Job  = new Job();
    job.jobType     = Job.JobTypeEnum.FaceRecognitionJob;
    job.jobProfile  = Job.JobProfileEnum.ExtractFaces;
    job.jobInput    = jobInput;

    let jobMediatorInput: JobMediatorInput  = new JobMediatorInput();
    jobMediatorInput.projectServiceId   = projectServiceId;
    jobMediatorInput.quantity           = inputMediaFile.duration;
    jobMediatorInput.job                = job;
    return jobMediatorInput;
};

const createMediatorClusterFacesJobInput = (faceExtractionId: any): JobMediatorInput => {

    let jobInput: ClusterFacesInput = new ClusterFacesInput();
    jobInput.jobInputType   = ClusterFacesInput.name;
	jobInput.faceExtractionId      = faceExtractionId as string;

    let job: Job  = new Job();
    job.jobType     = Job.JobTypeEnum.FaceRecognitionJob;
    job.jobProfile  = Job.JobProfileEnum.ClusterFaces;
    job.jobInput    = jobInput;

    let jobMediatorInput: JobMediatorInput  = new JobMediatorInput();
    jobMediatorInput.projectServiceId   = projectServiceId;
    jobMediatorInput.quantity           = inputMediaFile.duration;
    jobMediatorInput.job                = job;
    return jobMediatorInput;
};

const createMediatorSearchFacesJobInput = (clusterCollectionId: any, s3InputSignedUrl: string): JobMediatorInput => {

    let inputFile: Locator  = new Locator();
    inputFile.awsS3Bucket   = s3Bucket;
    inputFile.awsS3Key      = inputImageFile.name;
    inputFile.httpEndpoint  = s3InputSignedUrl;

    let jobInput: SearchFacesInput = new SearchFacesInput();
    jobInput.jobInputType   = SearchFacesInput.name;
    jobInput.inputImage      	= inputFile;
	jobInput.similarityThreshold = 0.8;
	jobInput.clusterCollectionId = clusterCollectionId;

    let job: Job  = new Job();
    job.jobType     = Job.JobTypeEnum.FaceRecognitionJob;
    job.jobProfile  = Job.JobProfileEnum.SearchFaces;
    job.jobInput    = jobInput;

    let jobMediatorInput: JobMediatorInput  = new JobMediatorInput();
    jobMediatorInput.projectServiceId   = projectServiceId;
    jobMediatorInput.quantity           = 10;
    jobMediatorInput.job                = job;
    return jobMediatorInput;
};



const getAccessToken = async (): Promise<Token> => {
    return (await authApi.getAccessToken(client, secret)).body;
};

const submitJob = async (jobInput: JobMediatorInput): Promise<MediatorJob> => {
    return (await jobsApi.createJob(jobInput)).body;
};

const waitForComplete = async (job: MediatorJob): Promise<MediatorJob> => {
    const delay = (ms: number) => {
        return new Promise( resolve => setTimeout(resolve, ms));
    };
    let mediatorStatus: JobMediatorStatus = job.status;
    while (mediatorStatus.status !== JobMediatorStatus.StatusEnum.COMPLETED
        && mediatorStatus.status !== JobMediatorStatus.StatusEnum.FAILED) {
        await delay(30000);
        job = (await jobsApi.getJobById(job.id)).body;
        mediatorStatus = job.status;
        console.log(mediatorStatus);
    }
	console.log("Received job status: " + mediatorStatus.status);
    return job;
};

const uploadMedia = async () => {
    const read: ReadStream = fs.createReadStream(inputMediaFile.folder + inputMediaFile.name);
    const params: AWS.S3.PutObjectRequest = {
        Bucket: s3Bucket,
        Key: inputMediaFile.name,
        Body: read
    };
    await s3.upload(params).promise();
};

const uploadImage = async () => {
    const read: ReadStream = fs.createReadStream(inputImageFile.folder + inputImageFile.name);
    const params: AWS.S3.PutObjectRequest = {
        Bucket: s3Bucket,
        Key: inputImageFile.name,
        Body: read
    };
    await s3.upload(params).promise();
};


const generateGetSignedUrl = () : string => {
    return s3.getSignedUrl('getObject', {
        "Bucket": s3Bucket,
        "Key": inputMediaFile.name
    });
};

const generateGetSignedUrlImage = () : string => {
    return s3.getSignedUrl('getObject', {
        "Bucket": s3Bucket,
        "Key": inputImageFile.name
    });
};



const main = async () => {
    try {
        console.log("Starting face recognition process ..");

        // Upload media to S3.
		console.log("Uploading media to s3 ...");
        await uploadMedia();
        console.log('media uploaded to s3 successfully');

        // Generate signed URL for input and output.
        const s3InputSignedUrl: string = generateGetSignedUrl();
        console.log(`Input file signed URL: ${s3InputSignedUrl}`);

        // Get API access-token for the this client.
		console.log('Receiving session token ...');
        const token: Token = await getAccessToken();
        console.log(token);

        // Update jobs API with the generated access token
        jobsApi.setApiKey(JobsApiApiKeys.tokenSignature, token.authorization);
        faceRecognitionApi.setApiKey(FaceRecognitionApiApiKeys.tokenSignature, token.authorization);

        // Create excract job
		console.log('Creating extract faces job');
        const mediatorJobInput: JobMediatorInput = createMediatorExtractFacesJobInput(s3InputSignedUrl);
        console.log(JSON.stringify(mediatorJobInput));

        // Submit the job to Mediator.
        const submittedJob: MediatorJob = await submitJob(mediatorJobInput);
        console.log(submittedJob);

        // Wait until job is done and get result.
        const completedJob = await waitForComplete(submittedJob);
        console.log(completedJob);

		const faceExtractionId: string = (completedJob.jobOutput as ExtractFacesOutput).faceExtractionId!;
		console.log("Extract faces job has finished successfully. Received faceExtractionId: " + faceExtractionId);

		// Get face extraction information
        console.log('Querying Face API for extraction info ...');
        const faceExtraction: FaceExtractionCollection = (await faceRecognitionApi.getFaceExtractionCollection(projectServiceId, faceExtractionId)).body;
        console.log(JSON.stringify(faceExtraction));

        // Get first face information
        console.log('Querying Face API for first face info ...');
        const face: Face = (await faceRecognitionApi.getFace(projectServiceId, faceExtraction.faceIds![0])).body;
        console.log(JSON.stringify(face));

        /// create cluster faces job
		console.log('Creating cluster faces job');
        const clusterJobInput: JobMediatorInput = createMediatorClusterFacesJobInput(faceExtractionId);
        console.log(JSON.stringify(clusterJobInput));

       // Submit the job to Mediator.
        const submittedClusterJob = await submitJob(clusterJobInput);
        console.log(submittedClusterJob);

        // Wait until job is done and get result.
        const completedClusterJob = await waitForComplete(submittedClusterJob);
        const clusterCollectionId: string = (completedClusterJob.jobOutput as ClusterFacesOutput).clusterCollectionId!;
        console.log(completedClusterJob);
 		console.log("Cluster faces job has finished successfully. clusterCollectionId is: " + clusterCollectionId);

        // Get cluster collection information
        console.log('Querying Face API for cluster collection info ...');
        const clusterCollection: ClusterCollection = (await faceRecognitionApi.getClusterCollection(projectServiceId, clusterCollectionId)).body;
        console.log(JSON.stringify(clusterCollection));

		console.log("Uploading image to s3 ...");
        await uploadImage();
        console.log('image is uploaded to s3 successfully');

        // Generate signed URL for input and output.
        const s3InputSignedUrlImage: string = generateGetSignedUrlImage();
        console.log(`Input file signed URL: ${s3InputSignedUrlImage}`);

       // Create excract job
		console.log('Creating search faces job');
        const searchJobInput: JobMediatorInput = createMediatorSearchFacesJobInput(clusterCollectionId, s3InputSignedUrlImage);
        console.log(JSON.stringify(searchJobInput));

        // Submit the job to Mediator.
        const submittedSearchJob: MediatorJob = await submitJob(searchJobInput);
        console.log(submittedSearchJob);

        // Wait until job is done and get result.
        const completedSearchJob = await waitForComplete(submittedSearchJob);
        console.log(completedSearchJob);

    } catch (e) {
        console.log(e);
    }
};

main().then();