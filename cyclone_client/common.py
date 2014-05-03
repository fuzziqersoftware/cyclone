class KeyMetadata(object):
  def __init__(self):
    self.archive_args = []
    self.x_files_factor = None
    self.agg_method = None

  def __repr__(self):
    return 'KeyMetadata(archive_args=%r, x_files_factor=%r, agg_method=%r)' % (
        self.archive_args, self.x_files_factor, self.agg_method)

  def __eq__(self, other):
    return isinstance(other, KeyMetadata) and \
        self.archive_args == other.archive_args and \
        self.x_files_factor == other.x_files_factor and \
        self.agg_method == other.agg_method
