# SMS Service -- Messages
`/messages` REST Endpoint

## Lambda

### Entry Point

- `src/main.handle`

### Environment Variables

- `TABLE_NAME`: AWS DynamoDB table name

## AWS Event Example

TODO: add example

## Contributing

- Install pyenv

```bash
brew install pyenv
```

- Add pyenv ENV to profile

```bash
echo "export PYENV_ROOT="$HOME/.pyenv"" >> ~/.zshrc
echo "export PATH="$PYENV_ROOT/bin:$PATH"" >> ~/.zshrc
```

- Install pipenv

```bash
brew install pipenv
```

- Add this environment variable so pipenv uses pyenv selected version when no version is specified

```bash
echo "export PIPENV_PYTHON="$PYENV_ROOT/shims/python"" >> ~/.zshrc
```

-  Install environment
```bash
pipenv install --dev
```

## Running tests

- Running tests
```bash
pipenv run pytest
```

- Running tests with file watch
```bash
pipenv run pytest --color=yes -f -s
```

## Build

Run build script:

```bash
sh ./build.sh
```

You should end up with a deployable zip `build/lambda.zip`
