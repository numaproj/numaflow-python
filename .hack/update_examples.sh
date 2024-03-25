#!/bin/bash

function show_help () {
    echo "Usage: $0 [-h|--help | -t|--tag <tag>] (-bp|--build-push | -bpe|--build-push-example <path> | -us|--update-sha <commit-sha> | -uv|--update-version <version>)"
    echo "  -h, --help                   Display help message and exit"
    echo "  -bp, --build-push            Build the Dockerfiles of all the examples and push them to the quay.io registry"
    echo "  -bpe, --build-push-example   Build the Dockerfile of the given example directory path, and push it to the quay.io registry"
    echo "  -t, --tag                    To be optionally used with -bpe or -bp. Specify the tag to build with. Default tag: stable"
    echo "  -us, --update-sha            Update all of the examples to depend on the specified commit SHA"
    echo "  -uv, --update-version        Update all of the examples to depend on the specified version"
}

function traverse_examples () {
  find examples -name "pyproject.toml"  | while read -r line;
  do
      dir="$(dirname "${line}")"
      cd "$dir" || exit
      # TODO: rewrite asyncio-reduce example using latest SDK version, as it is currently using old methods
      if [ "$dir" == "examples/developer_guide" ] || [ "$dir" == examples/reduce/asyncio-reduce ]; then
          cd ~- || exit
          continue
      fi

      command=$1
      if ! $command; then
        echo "Error: failed $command in $dir" >&2
        exit 1
      fi

      cd ~- || exit
  done
}

if [ $# -eq 0 ]; then
  echo "Error: provide at least one argument" >&2
  show_help
  exit 1
fi

usingHelp=0
usingBuildPush=0
usingBuildPushExample=0
usingSHA=0
usingVersion=0
usingTag=0
sha=""
version=""
directoryPath=""
tag="stable"

function handle_options () {
  while [ $# -gt 0 ]; do
    case "$1" in
      -h | --help)
        usingHelp=1
        ;;
      -bp | --build-push)
        usingBuildPush=1
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
      -us | --update-sha)
        if [ -z "$2" ]; then
          echo "Commit SHA not specified." >&2
          show_help
          exit 1
        fi

        usingSHA=1
        sha=$2
        shift
        ;;
      -uv | --update-version)
        if [ -z "$2" ]; then
          echo "Version not specified." >&2
          show_help
          exit 1
        fi

        usingVersion=1
        version=$2
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

if (( usingBuildPush + usingBuildPushExample + usingSHA + usingHelp + usingVersion > 1 )); then
  echo "Only one of '-h', '-bp', '-bpe', '-us', or '-uv' is allowed at a time" >&2
  show_help
  exit 1
fi

if (( (usingTag + usingSHA + usingHelp + usingVersion > 1) || (usingTag && usingBuildPush + usingBuildPushExample == 0) )); then
  echo "Can only use -t with -bp or -bpe" >&2
  show_help
  exit 1
fi

if [ -n "$sha" ]; then
 echo "Using SHA: $sha"
fi

if [ -n "$version" ]; then
 echo "Using version: $version"
fi

if [ -n "$directoryPath" ]; then
 echo "Dockerfile path to use: $directoryPath"
fi

if [ -n "$tag" ] && (( ! usingSHA )) && (( ! usingHelp )) && (( ! usingVersion )); then
 echo "Using tag: $tag"
fi

if (( usingBuildPush )); then
  traverse_examples "make image-push TAG=$tag"
elif (( usingBuildPushExample )); then
   cd "./$directoryPath" || exit
   if ! make image-push TAG="$tag"; then
     echo "Error: failed to run make image-push in $directoryPath" >&2
     exit 1
   fi
elif (( usingSHA )); then
  traverse_examples "poetry add git+https://github.com/numaproj/numaflow-python.git@$sha"
elif (( usingVersion )); then
  poetry version "$version"
  traverse_examples "poetry add pynumaflow@~$version"
elif (( usingHelp )); then
  show_help
fi
