<#
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
#>

# Sign a single DLL file in-place
function Invoke-SignFile {
    [OutputType([Boolean])]
    Param(
        # The path to the file to sign
        [Parameter(Mandatory=$true)]
        [string]$FilePath,
        # The name of the unsigned AWS bucket
        [Parameter(Mandatory=$true)]
        [string]$AwsUnsignedBucket,
        # The name of the signed AWS bucket
        [Parameter(Mandatory=$true)]
        [string]$AwsSignedBucket,
        # The key prefix of the bucket
        [Parameter(Mandatory=$true)]
        [string]$AwsBucketKeyPrefix,
        [Parameter(Mandatory=$false)]
        [bool]$AsMockResponse=$false
    )

    Write-Host "Signing file: $FilePath"

    if ($AsMockResponse) {
        Write-Host "Mock mode - skipping actual signing"
        return $true
    }

    $fileName = Split-Path $FilePath -Leaf
    $key = "$AwsBucketKeyPrefix$fileName"
    $maxRetries = 10

    # Upload unsigned file to S3
    Write-Host "Uploading unsigned file to S3"
    $versionId = aws s3api put-object --bucket $AwsUnsignedBucket --key $key --body $FilePath --acl bucket-owner-full-control --query VersionId --output text
    $versionId = $versionId.Trim('"')

    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed to upload unsigned file: $FilePath"
        return $false
    }

    # Get job ID from S3 object tagging
    Write-Host "Getting job ID from S3 tagging"
    for ( $i = 0; $i -lt 3; $i++ ) {
        # Get job ID
        $id=$( aws s3api get-object-tagging --bucket $AwsUnsignedBucket --key $key --version-id $versionId | jq -r '.TagSet[0].Value' )
        if ( $id -ne "null" ) {
            $jobId = $id
            break
        }

        Write-Host "Will sleep for 5 seconds between retries."
        Start-Sleep -Seconds 5
    }

    if ($jobId -eq "null") {
        Write-Host "Failed to get job ID for: $FilePath"
        return $false
    }

    # Download signed file
    Write-Host "Downloading signed file"
    $retryCount = 0
    do {
        aws s3api get-object --bucket $AwsSignedBucket --key "$key-$jobId" $FilePath
        $retryCount++
        if ($LASTEXITCODE -ne 0 -and $retryCount -le $maxRetries) {
            Start-Sleep -Seconds 2
        }
    } while ($LASTEXITCODE -ne 0 -and $retryCount -le $maxRetries)

    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed to download signed file: $FilePath"
        return $false
    }

    Write-Host "Successfully signed: $FilePath"
    return $true
}

function Invoke-SignDlls {
    [OutputType([Boolean])]
    Param(
        # The path to search for DLL files
        [Parameter(Mandatory=$true)]
        [string]$BuildPath,
        # The name of the unsigned AWS bucket
        [Parameter(Mandatory=$true)]
        [string]$AwsUnsignedBucket,
        [Parameter(Mandatory=$true)]
        # The name of the signed AWS bucket
        [string]$AwsSignedBucket,
        # The key prefix of the bucket
        [Parameter(Mandatory=$true)]
        [string]$AwsBucketKeyPrefix,
        [Parameter(Mandatory=$false)]
        [bool]$AsMockResponse=$false
    )

    # Find DLL files to sign
    $dllFiles = Get-ChildItem -Path $BuildPath -Filter "*.dll" -Recurse | Where-Object { 
        $_.FullName -match "\\bin\\Release\\" -and
        $_.Name -like "*AwsWrapperDataProvider*" 
    } | Select-Object -ExpandProperty FullName

    if ($dllFiles.Count -eq 0) {
        Write-Host "No DLL files found to sign"
        return $false
    }

    Write-Host "Found $($dllFiles.Count) DLL file(s) to sign"

    # Sign each DLL file
    foreach ($dllFile in $dllFiles) {
        if (!(Invoke-SignFile -FilePath $dllFile -AwsUnsignedBucket $AwsUnsignedBucket -AwsSignedBucket $AwsSignedBucket -AwsBucketKeyPrefix $AwsBucketKeyPrefix -AsMockResponse $AsMockResponse)) {
            Write-Host "Failed to sign: $dllFile"
            return $false
        }
    }

    Write-Host "All DLL files signed successfully"
    return $true
}
