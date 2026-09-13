FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/FCG.Outbox.Processor/FCG.Outbox.Processor.csproj src/FCG.Outbox.Processor/
RUN dotnet restore src/FCG.Outbox.Processor/FCG.Outbox.Processor.csproj
COPY . .
RUN dotnet publish src/FCG.Outbox.Processor/FCG.Outbox.Processor.csproj -c Release -o /app/publish --no-restore /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
USER $APP_UID
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "FCG.Outbox.Processor.dll"]
