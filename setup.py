import setuptools

# This script requires apache-beam, google-cloud-bigquery, google-cloud-storage, google-auth, google-auth-oauthlib,
# google-auth-httplib2, google-api-python-client, and pytz libraries.
# They will be installed automatically when the script is executed with the --setup_file option.
REQUIRED_PACKAGES = [
    'apache-beam[gcp]==2.45.0',
    'google-cloud-bigquery==3.5.0',
    'google-cloud-storage==2.7.0',
    'google-auth==2.16.1',
    'google-auth-oauthlib==1.0.0',
    'google-auth-httplib2==0.1.0',
    'google-api-python-client==2.78.0',
    'pytz==2022.7.1',
    'fire==0.5.0',
    'pytest==7.2.1'
]

setuptools.setup(
    name='dataflow',
    version='0.0.1',
    install_requires=REQUIRED_PACKAGES,
)
