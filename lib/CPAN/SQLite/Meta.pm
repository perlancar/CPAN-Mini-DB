package CPAN::SQLite::Meta;

# DATE
# VERSION

use 5.010001;
use strict;
use warnings;
use Log::Any '$log';

use Archive::Zip qw(:ERROR_CODES :CONSTANTS);
use Archive::Tar;
use File::Slurp::Tiny qw(read_file);
#use File::Temp qw(tempdir);
use JSON;
use YAML::Syck ();

use Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
                       index_cpan_meta
               );

our %SPEC;

sub _parse_json {
    my $content = shift;

    state $json = JSON->new;
    my $data;
    eval {
        $data = $json->decode($content);
    };
    if ($@) {
        $log->errorf("Can't parse JSON: %s", $@);
        return undef;
    } else {
        return $data;
    }
}

sub _parse_yaml {
    my $content = shift;

    my $data;
    eval {
        $data = YAML::Syck::Load($content);
    };
    if ($@) {
        $log->errorf("Can't parse YAML: %s", $@);
        return undef;
    } else {
        return $data;
    }
}

my %common_args = (
    cpan => {
        schema => 'str*',
        req => 1,
        summary => 'Location of your local CPAN mirror, e.g. /path/to/cpan',
    },
    db_dir => {
        summary => 'Database directory, defaults to your CPAN home',
        schema  => 'str*',
    },
    db_name => {
        summary => 'Database name',
        schema  => 'str*',
        default => 'cpandb.sql',
    },
);

$SPEC{'index_cpan_meta'} = {
    v => 1.1,
    summary => 'Create/update CPAN Meta index',
    args => {
        %common_args,
    },
};
sub index_cpan_meta {
    require DBI;

    my %args = @_;

    my $cpan    = $args{cpan} or return [412, "Please specify 'cpan'"];
    my $db_dir  = $args{db_dir} // $cpan;
    my $db_name = $args{db_name} // 'cpandb.sql';

    my $db_path = "$db_dir/$db_name";
    $log->tracef("Connecting to SQLite database at %s ...", $db_path);
    my $dbh = DBI->connect("dbi:SQLite:dbname=$db_path", undef, undef,
                           {RaiseError=>1});
    $dbh->do("CREATE TABLE IF NOT EXISTS files (
  file_id INTEGER NOT NULL PRIMARY KEY,
  file_name TEXT NOT NULL,
  status TEXT -- ok (indexed successfully), nometa (does not contain META.yml/META.json), nofile (file does not exist in local CPAN), unsupported (unsupported file type), err (other error, detail logged to Log::Any)
)");
    $dbh->do("CREATE INDEX IF NOT EXISTS ix_files_file_name ON files(file_name)");
    $dbh->do("CREATE TABLE IF NOT EXISTS deps (
  dep_id INTEGER NOT NULL PRIMARY KEY,
  file_id INTEGER,
  dist_id INTEGER,
  mod_id INTEGER, -- if release refers to a known module (listed in 'mods' table), only its id will be recorded here
  mod_name TEXT,  -- if release refers to an unknown module (unlisted in 'mods'), only the name will be recorded here
  rel TEXT, -- relationship: requires, ...
  phase TEXT, -- runtime, ...
  version TEXT,
  FOREIGN KEY (file_id) REFERENCES files(file_id),
  FOREIGN KEY (dist_id) REFERENCES dists(dist_id),
  FOREIGN KEY (mod_id) REFERENCES mods(mod_id)
)");
    $dbh->do("CREATE INDEX IF NOT EXISTS ix_deps_mod_name ON deps(mod_name)");

    my $sth;

    $dbh->begin_work;

    # list files
    $sth = $dbh->prepare("SELECT
  d.dist_id dist_id,
  dist_name,
  dist_file,
  cpanid
FROM dists d
  LEFT JOIN auths a USING(auth_id)
WHERE NOT EXISTS (SELECT 1 FROM files WHERE file_name=dist_file)
ORDER BY dist_file
");
    $sth->execute;
    my @files;
    while (my $row = $sth->fetchrow_hashref) {
        push @files, $row;
    }

    my $sth_insfile = $dbh->prepare("INSERT INTO files (file_name,status) VALUES (?,?)");

  FILE:
    for my $file (@files) {
        $log->tracef("Processing file %s ...", $file->{dist_file});
        my $status;
        my $path = "$cpan/authors/id/".substr($file->{cpanid}, 0, 1)."/".
            substr($file->{cpanid}, 0, 2)."/$file->{cpanid}/$file->{dist_file}";

        unless (-f $path) {
            $log->errorf("File %s doesn't exist, skipped", $file->{dist_file});
            $sth_insfile->execute($file->{dist_file}, "nofile");
            next FILE;
        }

        # try to get META.yml or META.json
        my $meta;
      GET_META:
        {
            unless ($path =~ /(.+)\.(tar|tar\.gz|tar\.bz2|tar\.Z|tgz|tbz2?|zip)$/i) {
                $log->errorf("Doesn't support file type: %s, skipped", $file->{dist_file});
                $sth_insfile->execute($file->{dist_file}, "unsupported");
                next FILE;
            }

            my ($name, $ext) = ($1, $2);
            if (-f "$name.meta") {
                $log->tracef("Getting meta from .meta file: %s", "$name.meta");
                eval { $meta = _parse_json(~~read_file("$name.meta")) };
                unless ($meta) {
                    $log->errorf("Can't read %s: %s", "$name.meta", $@) if $@;
                    $sth_insfile->execute($file->{dist_file}, "err");
                    next FILE;
                }
                last GET_META;
            }

            eval {
                if ($path =~ /\.zip$/i) {
                    my $zip = Archive::Zip->new;
                    $zip->read($path) == AZ_OK or die "Can't read zip file";
                    for my $member ($zip->members) {
                        if ($member->fileName =~ m!(?:/|\\)META.(yml|json)$!) {
                            #$log->tracef("  found %s", $member->fileName);
                            my $type = $1;
                            my $content = $zip->contents($member);
                            #$log->trace("[[$content]]");
                            if ($type eq 'yml') {
                                $meta = _parse_yaml($content);
                            } else {
                                $meta = _parse_json($content);
                            }
                            return; # from eval
                        }
                    }
                } else {
                    my $tar = Archive::Tar->new;
                    $tar->read($path);
                    for my $member ($tar->list_files) {
                        if ($member =~ m!/META\.(yml|json)$!) {
                            #$log->tracef("  found %s", $member);
                            my $type = $1;
                            my ($obj) = $tar->get_files($member);
                            my $content = $obj->get_content;
                            #$log->trace("[[$content]]");
                            if ($type eq 'yml') {
                                $meta = _parse_yaml($content);
                            } else {
                                $meta = _parse_json($content);
                            }
                            return; # from eval
                        }
                    }
                }
           }; # eval

            if ($@) {
                $log->errorf("Can't extract meta from file %s: %s", $path, $@);
                $sth_insfile->execute($file->{dist_file}, "err");
                next FILE;
            }
        } # GET_META

        unless ($meta) {
            $log->infof("File %s doesn't contain META.json/META.yml, skipped", $path);
            $sth_insfile->execute($file->{dist_file}, "nometa");
            next FILE;
        }

        $log->info("file=$path, distname=$meta->{name}");

    } # for file

    $dbh->commit;

    $log->tracef("Disconnecting from SQLite database ...");
    $dbh->disconnect;

    [200];
}

1;
# ABSTRACT:

=head1 SYNOPSIS

Before you use C<cpandb-meta>, you must already install CPAN::SQLite and create
its index, to do so:

 % cpanm CPAN::SQLite
 % cpandb --CPAN /path/to/cpan --db_dir /path/to/cpan --db_name cpandb.sql --setup

Afterwards, add information from CPAN Meta by doing this:

 % cpandb-meta --cpan /path/to/cpan index

Everytime you update your CPAN mirror, update the CPAN::SQLite index and also
the CPAN Meta information:

 % cpandb --CPAN /path/to/cpan --db_dir /path/to/cpan --db_name cpandb.sql --update
 % cpandb-meta --cpan /path/to/cpan index

To query CPAN Meta information in database:

 # find out the dependencies of a module (i.e. what modules are required by
 # Text::ANSITable)
 % cpandb-meta --cpan /path/to/cpan deps Text::ANSITable

 # find out the reverse dependencies of a module (i.e. which modules
 # (distributions) depends on Text::ANSITable)
 % cpandb-meta --cpan /path/to/cpan revdeps Text::ANSITable

For more options:

 % cpandb-meta --help
 % cpandb-meta index --help
 % cpandb-meta deps --help
 % cpandb-meta revdeps --help


=head1 DESCRIPTION

C<CPAN::SQLite::Meta> accompanies L<CPAN::SQLite> by adding information from
CPAN Meta files (C<META.json> or C<META.yml>) in the release files.

CPAN::SQLite only parses the C<$CPAN/modules/02packages.details.txt.gz> and
C<$CPAN/authors/01mailrc.txt.gz>. From these files these information can be
retrieved: package/module names, the list of authors (PAUSE ID's, names, and
emails), and the list of indexed release files/tarballs. Distribution names can
be guessed from the release files, but some files do not follow convention.

To get more information, one will need to parse the meta files (either from
C<*.meta> files in the authors directory, or by extracting C<META.yml> or
C<META.json> from release tarballs). This module does this and put the resulting
information to a few extra database tables. Currently the information
extracted/recorded are: dependencies (tables: C<files>, C<deps>).


=head1 SEE ALSO

Front-end for this module: L<cpandb-meta>.

L<CPAN::SQLite> and its front-end L<cpandb>.

L<CPANDB::Generator> CPANDB bills itself as a "unified database of CPAN metadata
information" and is a SQLite database that contains more information about CPAN
from various sources, e.g.: number of ratings from
L<http://cpanratings.perl.org>, upload date and test results from CPAN Testers,
number of tickets from L<http://rt.cpan.org>. However, generating CPANDB locally
will require downloading gigabytes of data and take hours. An online cache is
available and can be used via L<CPANDB> but you cannot control the update
frequency.

=cut
