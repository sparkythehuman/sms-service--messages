# Run tests
pipenv run pytest --color=yes

# Remove old build directories if present
rm -rf build/ > /dev/null 2>&1

# Create build folders
mkdir build
mkdir build/vendor

# Make requirement file from lock
pipenv lock -r > build/requirements.txt

# Install dependencies within build folder
pipenv run pip install -r build/requirements.txt --no-deps -t build/vendor/

# Zip source code
zip -r build/lambda.zip src

# Zip vendor libs
cd build/vendor/ && zip -r ../lambda.zip . > /dev/null 2>&1