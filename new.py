
from flask import Flask, jsonify, request
import boto3
import json


app = Flask(__name__)

@app.route('/create_glue_job', methods=['POST'])
def create_glue_job():
    try:
        # Read request JSON data
        req_data = request.get_json()
        input_path = req_data['input_path']
        output_path = req_data['output_path']
        job_name = req_data['jobname']
        aws_access_key = req_data['aws_access_key']
        aws_secret_key = req_data['aws_secret_key']
        Role = req_data['role']
        region = req_data['aws_region']
        bucket_name = req_data['bucket_name']
        object_key = req_data['object_key']

        
        

        # Read the content of the target script
        with open("fresh_script.py", "r") as f:
            content = f.read()

        # Replace the variable value
        updated_script = content.replace('<<INPUT_PATH>>', input_path )
        updated_script = updated_script.replace('<<OUTPUT_PATH>>', output_path)

        # Write the updated content back to the target script
        with open("fresh_script11.py", "w") as f:
            f.write(updated_script)

        print("Variables replaced successfully.")
        print(updated_script)

        #creating boto3 session
        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )

        # S3 client
        s3 = session.client('s3')


        # Upload the updated script to S3
        s3.upload_file("fresh_script11.py", bucket_name, object_key)

        print("Script uploaded to S3 successfully!")

                #creating boto3 session
        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )

        ###creating new glue job to run the script 
        glue  = session.client('glue')
        command = {
            'Name': 'glueetl',
            'ScriptLocation':f's3://{bucket_name}/{object_key}'
        }
        default_arguments = {
            '--job-language': 'python',
            '--enable-glue-datacatalog': '',
            '--job-bookmark-option': 'job-bookmark-enable'
        }
        allocated_capacity = 2

        response = glue.create_job(
            Name=job_name,
            Role=Role,
            Command=command,
            DefaultArguments=default_arguments,
            AllocatedCapacity=allocated_capacity,
            GlueVersion='4.0',
            MaxRetries=0
        )

        print(response)

        print(f"Created Glue job {job_name} with version {allocated_capacity}.")

        ############# Run glue job 

        glue.start_job_run(JobName=job_name)
        return jsonify({'message': 'Glue job creation successful'})

    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=False)
