from wtforms.validators import EqualTo
from wtforms.validators import ValidationError


class GreaterEqualThan(EqualTo):
    """Compares the values of two fields.

    :param fieldname:
        The name of the other field to compare to.
    :param message:
        Error message to raise in case of a validation error. Can be
        interpolated with `%(other_label)s` and `%(other_name)s` to provide a
        more helpful error.
    """

    def __call__(self, form, field):
        try:
            other = form[self.fieldname]
        except KeyError:
            raise ValidationError(
                field.gettext("Invalid field name '%s'." % self.fieldname))

        if field.data is None or other.data is None:
            return

        if field.data < other.data:
            d = {
                'other_label':
                hasattr(other, 'label') and other.label.text or self.fieldname,
                'other_name':
                self.fieldname,
            }
            message = self.message
            if message is None:
                message = field.gettext('Field must be greater than or equal '
                                        'to %(other_label)s.' % d)
            else:
                message = message % d

            raise ValidationError(message)


class SmallerEqualThan(EqualTo):
    """Compares the values of two fields.

    :param fieldname:
        The name of the other field to compare to.
    :param message:
        Error message to raise in case of a validation error. Can be
        interpolated with `%(other_label)s` and `%(other_name)s` to provide a
        more helpful error.
    """

    def __call__(self, form, field):
        try:
            other = form[self.fieldname]
        except KeyError:
            raise ValidationError(
                field.gettext("Invalid field name '%s'." % self.fieldname))

        if field.data is None or other.data is None:
            return

        if field.data > other.data:
            d = {
                'other_label':
                hasattr(other, 'label') and other.label.text or self.fieldname,
                'other_name':
                self.fieldname,
            }
            message = self.message
            if message is None:
                message = field.gettext('Field must be smallerr than or equal '
                                        'to %(other_label)s.' % d)
            else:
                message = message % d

            raise ValidationError(message)
