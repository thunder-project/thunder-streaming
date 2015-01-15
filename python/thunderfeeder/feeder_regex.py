import os
import re


class RegexMatchToQueueName(object):
    def __init__(self, regex_strings, queue_names):
        """

        regex_strings: sequence of string regular expressions
            if a regex match() occurs for a passed filename, then the queue name at the same index will be returned
            from queueName.
        """
        if len(regex_strings) != len(queue_names):
            raise ValueError("Must have equal numbers of regular expressions and queue names, " +
                             "got %d regexes and %d queues" % (len(regex_strings), len(queue_names)))
        self.regexs = [re.compile(regex_str) for regex_str in regex_strings]
        self.queue_names = list(queue_names)

    @classmethod
    def fromFile(cls, filename_or_handle):
        # expected format is file of:
        # queueName1 regex1
        # queueName2 regex2
        # with whitespace delimiting queue and regex
        def parse_file(handle):
            queue_names_ = []
            regexes_ = []
            for line in handle:
                if not line.strip().startswith('#'):
                    queue_name, regex = line.strip().split(None, 1)
                    queue_names_.append(queue_name)
                    regexes_.append(regex)
            return queue_names_, regexes_
        if isinstance(filename_or_handle, basestring):
            with open(filename_or_handle, 'r') as fp:
                queue_names, regexes = parse_file(fp)
        else:
            queue_names, regexes = parse_file(filename_or_handle)
        return cls(regexes, queue_names)

    def queueName(self, filename):
        basename = os.path.basename(filename)
        for qidx, regex in enumerate(self.regexs):
            if regex.match(basename):
                return self.queue_names[qidx]
        return None


def _first_noncomment_line(filename_or_handle):
    def parse_file(handle):
        for line in handle:
            if not line.strip().startswith('#'):
                # just read first non-comment line and return
                return line.strip()
        return ""
    if isinstance(filename_or_handle, basestring):
        with open(filename_or_handle, 'r') as fp:
            return parse_file(fp)
    else:
        return parse_file(filename_or_handle)


class RegexMatchToTimepointString(object):
    def __init__(self, regex_string, regex_group=1):
        self.regex = re.compile(regex_string)
        self.regex_group = regex_group

    @classmethod
    def fromFile(cls, filename_or_handle):
        return cls(_first_noncomment_line(filename_or_handle))

    def timepoint(self, filename):
        basename = os.path.basename(filename)
        m = self.regex.match(basename)
        return m.group(self.regex_group) if m else None


class RegexMatchToPredicate(object):
    def __init__(self, regex_string):
        self.regex = re.compile(regex_string)

    @classmethod
    def fromFile(cls, filename_or_handle):
        return cls(_first_noncomment_line(filename_or_handle))

    def predicate(self, filename):
        basename = os.path.basename(filename)
        return bool(self.regex.match(basename))
