import setuptools

# This script requires apache-beam, google-cloud-bigquery, google-cloud-storage, google-auth, google-auth-oauthlib,
# google-auth-httplib2, google-api-python-client, and pytz libraries.
# They will be installed automatically when the script is executed with the --setup_file option.
REQUIRED_PACKAGES = [
    'apache-beam[gcp]==2.30.0',
    'google-cloud-bigquery==2.26.0',
    'google-cloud-storage==1.38.0',
    'google-auth==1.35.0',
    'google-auth-oauthlib==0.4.5',
    'google-auth-httplib2==0.4.6',
    'google-api-python-client==2.30.0',
    'pytz==2021.3',
    'fire==0.5.0',
    'pytest==7.2.1'
]

setuptools.setup(
    name='dataflow',
    version='0.0.1',
    install_requires=REQUIRED_PACKAGES,
)
