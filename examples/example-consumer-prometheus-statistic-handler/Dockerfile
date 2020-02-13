FROM microsoft/dotnet:2.2-runtime
WORKDIR /app
COPY ./bin/Release/netcoreapp2.1/publish /app
ENTRYPOINT ["dotnet", "/app/example-consumer-prometheus-statistic-handler.dll"]