from rest_framework import serializers

class CommentsSerializer(serializers.Serializer):
    id = serializers.CharField()
    comment = serializers.CharField()
    author = serializers.CharField()
    timestamp = serializers.DateTimeField()