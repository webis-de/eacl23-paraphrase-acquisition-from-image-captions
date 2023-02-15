package de.webis.hadoop.formats.filter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.util.regex.Pattern;

public class GZipWarcFileFilter implements PathFilter {
    private static final Pattern FILE_PATTERN = Pattern.compile(".*\\.[a-z5]+?$");

    @Override
    public boolean accept(Path path) {
        boolean accept = true;

        if (FILE_PATTERN.matcher(path.toString()).matches()) {
            accept = path.getName().endsWith(".warc.gz");
        }

        return accept;
    }
}
