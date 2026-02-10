#!/usr/bin/env python3
import os
import logging
import argparse
import glob

from utils.fingerprint_unknown import fingerprint_type
from utils.load_parsers import load_parsers
from utils.logs import logger


def main():
    logger.info("Starting WTF parser")
    parsers = load_parsers()

    argparser = argparse.ArgumentParser(description='WTF Parser')
    argparser.add_argument('input', help='The file or folder to parse')

    argparser.add_argument('-o', '--output', help='Output path', required=False)
    argparser.add_argument('-p', '--parser', help='Specify which parser to use', choices=parsers.keys(), required=False)
    argparser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')

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
                    logger.info("Falling back to UnknownParser")
                    parser_class = parsers["unknown"]
                else:
                    logger.error("No generic parser found. Skipping file.")
                    return

            parser = parser_class(file, output_path=args.output)
            parser.parse()
        except Exception as e:
            logger.error(f"Error processing file: {file}\nError: {e}")

if __name__ == "__main__":
    main()