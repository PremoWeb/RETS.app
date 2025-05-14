interface RetsResponse {
  ReplyCode: string;
  ReplyText: string;
  Metadata?: {
    Type: string;
    Version?: string;
    Date?: string;
    Data: any[];
  };
  [key: string]: any;
}

export class RetsParser {
  private static parseMetadataType(xmlString: string): string | null {
    const lines = xmlString.split('\n');
    if (lines.length < 2) return null;
    
    const metadataMatch = lines[1].match(/<METADATA-([^>\s]+)/);
    return metadataMatch ? metadataMatch[1] : null;
  }

  private static parseAttributes(tag: string): { [key: string]: string } {
    const attrs: { [key: string]: string } = {};
    const matches = tag.match(/(\w+)="([^"]+)"/g);
    
    if (matches) {
      matches.forEach(match => {
        const [key, value] = match.replace(/"/g, '').split('=');
        attrs[key] = value;
      });
    }
    
    return attrs;
  }

  private static parseTabDelimitedData(columns: string, data: string): { [key: string]: string } {
    const columnNames = columns.split('\t').map(col => col.trim()).filter(Boolean);
    const values = data.split('\t').map(val => val.trim());
    
    const result: { [key: string]: string } = {};
    columnNames.forEach((col, index) => {
      result[col] = values[index] || '';
    });
    
    return result;
  }

  private static parseLoginResponse(xmlString: string): RetsResponse {
    const replyCodeMatch = xmlString.match(/ReplyCode="([^"]+)"/);
    const replyTextMatch = xmlString.match(/ReplyText="([^"]+)"/);
    const responseMatch = xmlString.match(/<RETS-RESPONSE>([\s\S]*?)<\/RETS-RESPONSE>/);
    
    if (!replyCodeMatch || !replyTextMatch) {
      throw new Error('Invalid RETS response format');
    }

    const response: RetsResponse = {
      ReplyCode: replyCodeMatch[1],
      ReplyText: replyTextMatch[1]
    };

    if (responseMatch) {
      const urls: { [key: string]: string } = {};
      const lines = responseMatch[1].split('\n');
      
      lines.forEach(line => {
        if (line.includes('=')) {
          const [key, value] = line.split('=').map(s => s.trim());
          if (key && value && !key.startsWith('Info')) {
            urls[key] = value;
          }
        }
      });
      
      response['RETS-RESPONSE'] = urls;
    }

    return response;
  }

  private static parseMetadataResponse(xmlString: string, metadataType: string): RetsResponse {
    const replyCodeMatch = xmlString.match(/ReplyCode="([^"]+)"/);
    const replyTextMatch = xmlString.match(/ReplyText="([^"]+)"/);
    const metadataMatch = xmlString.match(new RegExp(`<METADATA-${metadataType}([^>]+)>`));
    
    if (!replyCodeMatch || !replyTextMatch || !metadataMatch) {
      throw new Error('Invalid metadata response format');
    }

    const response: RetsResponse = {
      ReplyCode: replyCodeMatch[1],
      ReplyText: replyTextMatch[1],
      Metadata: {
        Type: metadataType,
        ...this.parseAttributes(metadataMatch[1]),
        Data: []
      }
    };

    // Parse COLUMNS
    const columnsMatch = xmlString.match(/<COLUMNS>([^<]+)<\/COLUMNS>/);
    if (!columnsMatch) {
      throw new Error('No COLUMNS found in metadata response');
    }
    const columns = columnsMatch[1].trim();

    // Parse all DATA sections
    const dataMatches = xmlString.matchAll(/<DATA>([^<]+)<\/DATA>/g);
    for (const match of dataMatches) {
      const parsedData = this.parseTabDelimitedData(columns, match[1].trim());
      response.Metadata!.Data.push(parsedData);
    }

    return response;
  }

  public static async parse(xmlString: string): Promise<RetsResponse> {
    // Clean up the input string
    xmlString = xmlString.trim();

    // Check if this is a login response
    if (xmlString.includes('<RETS-RESPONSE>')) {
      return this.parseLoginResponse(xmlString);
    }

    // Check if this is a metadata response
    const metadataType = this.parseMetadataType(xmlString);
    if (metadataType) {
      return this.parseMetadataResponse(xmlString, metadataType);
    }

    // Handle RETS search responses with <COLUMNS> and <DATA>
    const columnsMatch = xmlString.match(/<COLUMNS>([\s\S]*?)<\/COLUMNS>/);
    const dataMatches = [...xmlString.matchAll(/<DATA>([\s\S]*?)<\/DATA>/g)];
    if (columnsMatch && dataMatches.length > 0) {
      const replyCodeMatch = xmlString.match(/ReplyCode="([^"]+)"/);
      const replyTextMatch = xmlString.match(/ReplyText="([^"]+)"/);
      const countMatch = xmlString.match(/<COUNT[^>]*Records="(\d+)"/);
      const columns = columnsMatch[1].replace(/^[\s\t]+|[\s\t]+$/g, '').split(/\t|\u0009/).filter(Boolean);
      const records = dataMatches.map(match => {
        const values = match[1].replace(/^[\s\t]+|[\s\t]+$/g, '').split(/\t|\u0009/);
        const obj: { [key: string]: string } = {};
        columns.forEach((col, idx) => {
          obj[col] = values[idx] || '';
        });
        return obj;
      });
      return {
        ReplyCode: replyCodeMatch ? replyCodeMatch[1] : '',
        ReplyText: replyTextMatch ? replyTextMatch[1] : '',
        Count: countMatch ? parseInt(countMatch[1], 10) : records.length,
        records
      };
    }

    // If we get here, try to parse as a generic RETS response
    const replyCodeMatch = xmlString.match(/ReplyCode="([^"]+)"/);
    const replyTextMatch = xmlString.match(/ReplyText="([^"]+)"/);
    
    if (!replyCodeMatch || !replyTextMatch) {
      throw new Error('Invalid RETS response format');
    }

    return {
      ReplyCode: replyCodeMatch[1],
      ReplyText: replyTextMatch[1]
    };
  }

  public static isUnauthorizedQuery(response: RetsResponse): { resource: string, className: string } | null {
    if (response.ReplyCode === '20207' && response.ReplyText.includes('Unauthorized Query')) {
      // Try to extract class and resource from the ReplyText
      const match = response.ReplyText.match(/class \[([^\]]+)\] in resource \[([^\]]+)\]/);
      if (match) {
        return { className: match[1], resource: match[2] };
      }
    }
    return null;
  }
} 