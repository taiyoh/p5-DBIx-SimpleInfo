package DBIx::SimpleInfo;
use 5.008_001;
use strict;
use warnings;

our $VERSION = '0.01';

use Carp ();

sub new {
    my $package = shift;
    my %args = @_ > 1 ? @_ : %{$_[0]};

    my $self = bless(\%args, $package);

    Carp::croak("require 'connect_info' or 'dbh'")
        unless $args{dbh} || $args{connect_info};

    $self->connect unless $args{dbh};

    my $dbh = $self->{dbh};
    $self->{vendor}  = uc $dbh->get_info(17);
    $self->{version} = $dbh->get_info(18);
    $self->{schema}  = ($dbh->get_info(2) =~ /^.+?:.+?:(.+)($|[;:])/)[0];

    $self->{table} = [];
    my $sth = $dbh->table_info(undef, $self->{schema}, '%', "'TABLE','VIEW'");
    while (my $table_info = $sth->fetchrow_hashref) {
      push @{ $self->{table} }, $table_info->{TABLE_NAME};
    }

    $self;
}

sub connect {
    my $self = shift;
    my $info = $self->{connect_info};
    $self->{dbh} = eval { require DBI; DBI->connect(@$info) };
}

my %column_fetcher = (
  MYSQL => q[SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{:schema}' AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION]
);

sub column_info {
    my $self = shift;
    my $info = {};
    for my $table (@{ $self->{table} }) {
        $info->{$table} = $self->column_info_table($table);
    }
    return $info;
}

sub column_info_table {
    my $self  = shift;
    my $table = shift or return;
    return unless scalar grep { $_ eq $table } @{ $self->{table} };

    my $sql = $column_fetcher{$self->{vendor}};
    my @binds;
    if ($self->{vendor} eq 'MYSQL') {
        $sql =~ s/{:schema}/$self->{schema}/;
        push @binds, $table;
    }
    my $sth = $self->{dbh}->prepare($sql);
    $sth->execute(@binds);

    my @info;
    while (my $col_info = $sth->fetchrow_hashref) {
        push @info, {
            COLUMN_NAME   => $col_info->{COLUMN_NAME},
            POSITION      => $col_info->{ORDINAL_POSITION},
            COLUMN_TYPE   => $col_info->{COLUMN_TYPE},
            CHARSET       => $col_info->{CHARACTER_SET_NAME} || "",
            CHAR_LENGTH   => $col_info->{CHARACTER_MAXIMUM_LENGTH} || "",
            IS_NULLABLE   => $col_info->{IS_NULLABLE},
            COLUMN_KEY    => $col_info->{COLUMN_KEY} || "",
            COMMENT       => $col_info->{COLUMN_COMMENT} || "",
            DEFAULT_VALUE => $col_info->{COLUMN_DEFAULT},
            EXTRA         => $col_info->{EXTRA} || ""
        };
    }
    return \@info;
}

1;
__END__

=head1 NAME

DBIx::SimpleInfo - Perl extention to do something

=head1 VERSION

This document describes DBIx::SimpleInfo version 0.01.

=head1 SYNOPSIS

    use DBIx::SimpleInfo;

=head1 DESCRIPTION

# TODO

=head1 INTERFACE

=head2 Functions

=head3 C<< hello() >>

# TODO

=head1 DEPENDENCIES

Perl 5.8.1 or later.

=head1 BUGS

All complex software has bugs lurking in it, and this module is no
exception. If you find a bug please either email me, or add the bug
to cpan-RT.

=head1 SEE ALSO

L<perl>

=head1 AUTHOR

taiyoh E<lt>sun.basix@gmail.comE<gt>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2013, taiyoh. All rights reserved.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
