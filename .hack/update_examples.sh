#!/bin/bash

function show_help () {
    echo "Usage: $0 [-h|--help | -t|--tag <tag>] (-bpe|--build-push-example <path> | -u|--update <path>)"
    echo "  -h, --help                   Display help message and exit"
    echo "  -bpe, --build-push-example   Build the Dockerfile of the given example directory path, and push it to the quay.io registry"
    echo "  -t, --tag                    To be optionally used with -bpe. Specify the tag to build with. Default tag: stable"
    echo "  -u, --update                 Create a dist/ folder which contains a tarball of the SDK, in the specified example directory"
}

if [ $# -eq 0 ]; then
  echo "Error: provide at least one argument" >&2
  show_help
  exit 1
fi

usingHelp=0
usingBuildPushExample=0
usingUpdate=0
usingTag=0
directoryPath=""
tag="stable"

function handle_options () {
  while [ $# -gt 0 ]; do
    case "$1" in
      -h | --help)
        usingHelp=1
        ;;
      -bpe | --build-push-example)
        if [ -z "$2" ]; then
          echo "Directory path not specified." >&2
          show_help
          exit 1
        fi

        usingBuildPushExample=1
        directoryPath=$2
        shift
        ;;
      -t | --tag)
        if [ -z "$2" ]; then
          echo "Tag not specified." >&2
          show_help
          exit 1
        fi

        usingTag=1
        tag=$2
        shift
        ;;
      -u | --update)
        if [ -z "$2" ]; then
          echo "Directory path not specified." >&2
          show_help
          exit 1
        fi

        usingUpdate=1
        directoryPath=$2
        shift
        ;;
      *)
        echo "Invalid option: $1" >&2
        show_help
        exit 1
        ;;
    esac
    shift
  done
}

handle_options "$@"

if (( usingBuildPushExample + usingUpdate + usingHelp  > 1 )); then
  echo "Only one of '-h', '-bpe', or '-u' is allowed at a time" >&2
  show_help
  exit 1
fi

if (( (usingTag + usingHelp + usingUpdate > 1) || (usingTag && usingBuildPushExample == 0) )); then
  echo "Can only use -t with -bpe" >&2
  show_help
  exit 1
fi

if [ -n "$tag" ] && (( ! usingUpdate )) && (( ! usingHelp )); then
 echo "Using tag: $tag"
fi

if (( usingBuildPushExample )); then
   cd "./$directoryPath" || exit
   if ! make image-push TAG="$tag"; then
     echo "Error: failed to run make image-push in $directoryPath" >&2
     exit 1
   fi
elif (( usingUpdate )); then
  if [ ! -d "$directoryPath" ]; then
    echo "Error: the specified directory path does not exist" >&2
    exit 1
  fi

  if [ -d dist ]; then
    rm -rf ./dist
  fi

  if ! poetry build --format sdist; then
    echo "Error: failed to build pynumaflow in $directoryPath" >&2
    exit 1
  fi

  if ! mv dist/"$(ls dist)" dist/pynumaflow.tar.gz; then
    echo "Error: failed to rename pynumaflow tarball in $directoryPath" >&2
    exit 1
  fi

  if ! cp -r dist "$directoryPath"/; then
    echo "Error: failed to copy over dist folder to $directoryPath" >&2
    exit 1
  fi
elif (( usingHelp )); then
  show_help
fi

