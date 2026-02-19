#!/usr/bin/python3
import os
import logging
import argparse
import glob
import traceback

from utils.fingerprint_unknown import fingerprint_type
from utils.load_parsers import load_parsers
from utils.logs import get_logger

logger = get_logger(__name__)

def main():
    logger.info("Starting WTF parser")
    parsers = load_parsers()

    argparser = argparse.ArgumentParser(description='WTF Parser')
    argparser.add_argument('input', help='The file or folder to parse')

    argparser.add_argument('-o', '--output', help='Output path', required=False)
    argparser.add_argument('-p', '--parser', help='Specify which parser to use', choices=parsers.keys(), required=False)
    argparser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    argparser.add_argument('-s', '--source', help='Name of the data source (stored as "source" field)', required=False)
    argparser.add_argument('-t', '--threads', type=int, default=1, help='Number of threads for parallel parsing')
    argparser.add_argument('-n', '--no-output', action="store_true", help="Do not keep the output file, this is useful for development")
    argparser.add_argument('--headers', default="", type=str, required=False)
    argparser.add_argument('--dry-run', action='store_true', help='Only parse 1000 lines from input files. This is useful for testing your parser on a large dataset before converting.')
    argparser.add_argument('--recency-year', default=None, type=int, required=False)

    args = argparser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")

    logger.debug(f"Loaded {len(parsers)} parser(s)")

    files = []

    if os.path.isdir(args.input):
        logger.info(f"Input is a directory. Processing all files in: {args.input}")
        for file_path in glob.glob(os.path.join(args.input, '**/*'), recursive=True):
            if os.path.isfile(file_path):
                files.append(file_path)

        if args.output:
            # Create output directory if it doesn't exist
            os.makedirs(args.output, exist_ok=True)

    elif os.path.isfile(args.input):
        logger.info(f"Input is a file. Processing: {args.input}")
        files.append(args.input)

    for file in files:
        logger.info(f"Processing file: {file}")
        try:
            file_extension = os.path.splitext(file)[1].lower()
            parser_class = None
            if not args.parser:
                for _, cls in parsers.items():
                    if file_extension in cls._EXTENSIONS:
                        parser_class = cls
                        logger.info(f"Auto-detected parser: {cls.__name__} for extension {file_extension}")
                        break

                if not parser_class:
                    logger.error(f"No parser found for file extension: {file_extension}")
                    # Attempt to fingerprint the file and find a matching parser
                    fingerprint = fingerprint_type(file)
                    logger.info(f"Fingerprint for file {file}: {fingerprint}")

                    if fingerprint in parsers:
                        parser_class = parsers[fingerprint]
                        logger.info(f"Found parser {parser_class.__name__} for fingerprint: {fingerprint}")
                    elif parsers.get("unknown"):
                        if (file_extension == '.7z'):
                            logger.warning("Skipping archive %s", file)
                            continue

                        logger.info("Falling back to UnknownParser")
                        parser_class = parsers["unknown"]
                    else:
                        logger.error("No generic parser found. Skipping file.")
                        return
            else:
                if args.parser in parsers:
                    parser_class = parsers[args.parser]
                else:
                    logger.warning(f"Invalid parser choice: {args.parser} not in {parsers}")
                    exit(-1)

            parser = parser_class(file, args=args)
            parser.parse()
        except Exception as e:
            logger.error(f"Error processing file: %s\nError: %s", file, traceback.format_exc())

if __name__ == "__main__":
    main()