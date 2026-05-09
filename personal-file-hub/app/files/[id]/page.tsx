import { notFound } from "next/navigation";
import filesData from "@/data/files.json";
import FilePreview from "@/components/FilePreview";
import { FileMetadata } from "@/types";

interface Props {
  params: { id: string };
}

export default function FileDetailPage({ params }: Props) {
  const files = filesData as FileMetadata[];
  const file = files.find((f) => f.id === params.id);

  if (!file) notFound();

  return (
    <div className="h-full flex flex-col">
      <FilePreview file={file} />
    </div>
  );
}

export async function generateStaticParams() {
  const files = filesData as FileMetadata[];
  return files.map((f) => ({ id: f.id }));
}
