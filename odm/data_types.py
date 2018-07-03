"""
DataTypes module.
It contains all the basic data types.
"""


class Types:
    """
    Types class.

    """
    String = "String"
    ISODate = "ISODate"
    Integer = "Integer"
    Double = "Double"
    Object = "Object"
    Array = "Array"
    ObjectId = "ObjectId"
    Boolean = "Boolean"


class Relations:
    """
    Relations class.

    """
    hasMany = "hasMany"
    hasOne = "hasOne"
    belongsToMany = "belongsToMany"
    belongsTo = "belongsTo"
    manyToMany = "manyToMany"
